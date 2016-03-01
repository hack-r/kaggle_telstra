# One Y per level
train.n$Y_0 <- 0
train.n$Y_0[train.n$fault_severity == 0] <- 1
train.n$Y_1 <- 0
train.n$Y_1[train.n$fault_severity == 1] <- 1
train.n$Y_2 <- 0
train.n$Y_2[train.n$fault_severity == 2] <- 1

# Load the package, start H2O
require(h2oEnsemble)
trainHex <- as.h2o(train.n, destination_frame="train_tel.hex")
testHex  <- as.h2o(test.n,  destination_frame="test_tel.hex")

trainHex[,"fault_severity_factor"] <- as.factor(trainHex[,"fault_severity"])

trainHex[,"location"]     <- as.factor(trainHex[,"location"])
trainHex[,"fit3_factor"]  <- as.factor(trainHex[,"fit3.cluster"])
trainHex[,"fit20_factor"] <- as.factor(trainHex[,"fit20.cluster"])

testHex[,"location"]     <- as.factor(testHex[,"location"])
testHex[,"fit2a_factor"]  <- as.factor(testHex[,"fit2a.cluster"])
testHex[,"fit3a_factor"]  <- as.factor(testHex[,"fit3a.cluster"])
testHex[,"fit5a_factor"]  <- as.factor(testHex[,"fit5a.cluster"])
testHex[,"fit3_factor"]  <- as.factor(testHex[,"fit3.cluster"])
testHex[,"fit20_factor"] <- as.factor(testHex[,"fit20.cluster"])

trainHex[,"Y_0"] <- as.factor(trainHex[,"Y_0"])
trainHex[,"Y_1"] <- as.factor(trainHex[,"Y_1"])
trainHex[,"Y_2"] <- as.factor(trainHex[,"Y_2"])

# Stage 1 Models
h2o.init(nthreads=-1)
learner <- c("h2o.glm.wrapper", "h2o.randomForest.wrapper",
             "h2o.gbm.wrapper", "h2o.deeplearning.wrapper")

x_names <- colnames(testHex)
x_names <- x_names[!(x_names == "id")]
x_names <- x_names[!(x_names %in% a$variable[a$percentage < .001])]

fit.y0 <- h2o.ensemble(x              = x_names,
                       y              = "Y_0",
                       training_frame = trainHex,
                       learner        = learner,
                       metalearner    = "h2o.glm.wrapper",#metalearner <- "SL.nnls"
                       parallel       = "multicore",
                       cvControl      = list(V=5))
fit.y0

fit.y1 <- h2o.ensemble(x              = x_names,
                       y              = "Y_1",
                       training_frame = trainHex,
                       learner        = learner,
                       metalearner    = "h2o.glm.wrapper",#metalearner <- "SL.nnls"
                       parallel       = "multicore",
                       cvControl      = list(V=5))
fit.y1

fit.y2 <- h2o.ensemble(x              = x_names,
                       y              = "Y_2",
                       training_frame = trainHex,
                       learner        = learner,
                       metalearner    = "h2o.glm.wrapper",#metalearner <- "SL.nnls"
                       parallel       = "multicore",
                       cvControl      = list(V=5))
fit.y2


# Generate predictions on the training set
pred0   <- predict(fit.y0, trainHex)
pred1   <- predict(fit.y1, trainHex)
pred2   <- predict(fit.y2, trainHex)
labels0 <- as.data.frame(train.n$Y_0)[,1]
labels1 <- as.data.frame(train.n$Y_1)[,1]
labels2 <- as.data.frame(train.n$Y_2)[,1]

pred0.test   <- predict(fit.y0, testHex)
pred1.test   <- predict(fit.y1, testHex)
pred2.test   <- predict(fit.y2, testHex)

# Ensemble test AUC
auc <- as.data.frame(pred0$pred)[,3]
AUC(predictions=as.numeric(auc), labels=labels0)
auc <- as.data.frame(pred1$pred)[,3]
AUC(predictions=as.numeric(auc), labels=labels1)
auc <- as.data.frame(pred2$pred)[,3]
AUC(predictions=as.numeric(auc), labels=labels2)
# 0.9529252 for Label 0
# 0.9530592 for Label 1
# 0.9877254 for Label 2

# Base learner test AUC (for comparison)
L <- length(learner)
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred0$basepred)[,l], labels = labels0))
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred1$basepred)[,l], labels = labels1))
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred2$basepred)[,l], labels = labels2))
# Label 0: [1] 0.9376163 0.9594962 0.9421740 0.9108918
# Label 1: [1] 0.9257349 0.9595132 0.9309237 0.9014940
# Label 2: [1] 0.9768262 0.9884636 0.9844859 0.9582718

# Note RF base did as well, or slightly better, than the GLM meta learner, however
#   I am trying using the GLM metalearners based on the paper I just read

# Add predictions to data
train.predictions <- cbind(as.data.frame(pred0$pred[,3]),
                           as.data.frame(pred1$pred[,3]),
                           as.data.frame(pred2$pred[,3])
                           )

setnames(train.predictions, c("pr_label0_ensemble",
                              "pr_label1_ensemble",
                              "pr_label2_ensemble")
        )

test.predictions <- cbind(as.data.frame(pred0.test$pred[,3]),
                          as.data.frame(pred1.test$pred[,3]),
                          as.data.frame(pred2.test$pred[,3])
                          )

setnames(test.predictions, c("pr_label0_ensemble",
                             "pr_label1_ensemble",
                             "pr_label2_ensemble")
        )

train.n <- cbind(train.n, train.predictions)
test.n  <- cbind(test.n, test.predictions)

saveRDS(train.n, "train.n.level1.RDS")
saveRDS(test.n,  "test.n.level1.RDS")

# Stage 2 Model (same as in tel.R)
trainHex <- as.h2o(train.n, destination_frame="train_tel.hex")
testHex  <- as.h2o(test.n,  destination_frame="test_tel.hex")

trainHex[,"fault_severity_factor"] <- as.factor(trainHex[,"fault_severity"])

trainHex[,"location"]      <- as.factor(trainHex[,"location"])
trainHex[,"fit2a_factor"]  <- as.factor(trainHex[,"fit2a.cluster"])
trainHex[,"fit3a_factor"]  <- as.factor(trainHex[,"fit3a.cluster"])
trainHex[,"fit5a_factor"]  <- as.factor(trainHex[,"fit5a.cluster"])
trainHex[,"fit3_factor"]   <- as.factor(trainHex[,"fit3.cluster"])
trainHex[,"fit20_factor"]  <- as.factor(trainHex[,"fit20.cluster"])

testHex[,"location"]      <- as.factor(testHex[,"location"])
testHex[,"fit2a_factor"]  <- as.factor(testHex[,"fit2a.cluster"])
testHex[,"fit3a_factor"]  <- as.factor(testHex[,"fit3a.cluster"])
testHex[,"fit5a_factor"]  <- as.factor(testHex[,"fit5a.cluster"])
testHex[,"fit3_factor"]   <- as.factor(testHex[,"fit3.cluster"])
testHex[,"fit20_factor"]  <- as.factor(testHex[,"fit20.cluster"])

x_names <- colnames(testHex)
x_names <- x_names[!(x_names == "id")]
#x_names <- x_names[!(x_names %in% a$variable[a$percentage < .001])]
length(x_names)

glmHex <- h2o.glm(        x                 = x_names,
                          y                 = "fault_severity_factor",
                          training_frame    = trainHex,
                          model_id          = "telstra_level2_glm.hex",
                          solver            = "L_BFGS",
                          family            = "multinomial",
                          ignore_const_cols = T,
                          nfold             = 5,
                          standardize       = T,
                          alpha             = 0)
# ,
# lambda_search     = T,
# nlambdas          = 10

glmHex
h2o.mse(glmHex)
sqrt(h2o.mse(glmHex)) # Level 2 Ridge GlM had 5-fold CV RMSE of .4879109
                      #   actual LB RMSE **0.74474**,
                      #   level 1 logit LB RMSE .61095
h2o.varimp(glmHex)
#a <- as.data.frame(h2o.varimp(rfHex))
#saveRDS(a, "a.RDS")

rm(stage1.drf.pred)
stage1.drf.pred <- as.data.frame(h2o.predict(glmHex, testHex))

# Submission --------------------------------------------------------------
# For classification:
rm(sub)
stage1.drf.pred$predict <- NULL
sub <- cbind(sample$id, stage1.drf.pred)
setnames(sub, colnames(sample))
head(sub)
write.csv(sub, "tel_sub_level2_ridge.csv", row.names = F)

# For regression:
sub <- sample
sub$predict_1 <- 0
sub$predict_0[stage1.drf.pred < .5] <- 1
sub$predict_1[stage1.drf.pred < 1.5 & stage1.drf.pred > .5] <- 1
sub$predict_2[stage1.drf.pred > 1.5] <- 1
summary(sub)
summary(train$fault_severity)
write.csv(sub, "tel_sub.csv", row.names = F)

# Level 1 (not level 2) Submission
rm(sub)
sub <- cbind(sample$id, test.predictions)
setnames(sub, colnames(sample))
head(sub)
write.csv(sub, "tel_sub_level1_logit.csv", row.names = F)
