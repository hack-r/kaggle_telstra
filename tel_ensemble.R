# One Y per level
train.n$Y_0 <- 0
train.n$Y_0[train.n$fault_severity == 0] <- 1
train.n$Y_1 <- 0
train.n$Y_1[train.n$fault_severity == 1] <- 1
train.n$Y_2 <- 0
train.n$Y_2[train.n$fault_severity == 2] <- 1

train.n$pr_label0_ensemble <- NULL
train.n$pr_label1_ensemble <- NULL
train.n$pr_label2_ensemble <- NULL
test.n$pr_label0_ensemble <- NULL
test.n$pr_label1_ensemble <- NULL
test.n$pr_label2_ensemble <- NULL

# Load the package, start H2O
require(h2oEnsemble)
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

trainHex[,"Y_0"] <- as.factor(trainHex[,"Y_0"])
trainHex[,"Y_1"] <- as.factor(trainHex[,"Y_1"])
trainHex[,"Y_2"] <- as.factor(trainHex[,"Y_2"])

# Stage 1 Models (level 0 data, level 0 -> level 1 models)
h2o.init(nthreads=-1)
learner <- c("h2o.glm.wrapper", "h2o.randomForest.wrapper",
             "h2o.gbm.wrapper", "h2o.deeplearning.wrapper")

x_names <- colnames(testHex)
x_names <- x_names[!(x_names == "id")]
#x_names <- x_names[!(x_names %in% a$variable[a$percentage < .001])]

fit.y0 <- h2o.ensemble(x              = x_names,
                    y              = "Y_0",
                    training_frame = trainHex,
                    learner        = learner,
                    metalearner    = "h2o.randomForest.wrapper",#metalearner <- "SL.nnls"
                    parallel       = "multicore",
                    cvControl      = list(V=5))
fit.y0

fit.y1 <- h2o.ensemble(x              = x_names,
                       y              = "Y_1",
                       training_frame = trainHex,
                       learner        = learner,
                       metalearner    = "h2o.randomForest.wrapper",#metalearner <- "SL.nnls"
                       parallel       = "multicore",
                       cvControl      = list(V=5))
fit.y1

fit.y2 <- h2o.ensemble(x              = x_names,
                       y              = "Y_2",
                       training_frame = trainHex,
                       learner        = learner,
                       metalearner    = "h2o.randomForest.wrapper",#metalearner <- "SL.nnls"
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
# 0.9241612 for Label 0
# 0.9170709 for Label 1
# 0.9719304 for Label 2

# Base learner test AUC (for comparison)
L <- length(learner)
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred0$basepred)[,l], labels = labels0))
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred1$basepred)[,l], labels = labels1))
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred2$basepred)[,l], labels = labels2))
# Label 0: 0.9373509 0.9664716 0.9418423 0.9160086
# Label 1: 0.9257303 0.9656398 0.9280820 0.9091131
# Label 2: 0.9769017 0.9896428 0.9849305 0.9679625

# Seems 1st stage RF is the best performer

# Add predictions to data
train.predictions <- cbind(as.data.frame(pred0$pred[,3]),
                           as.data.frame(pred1$pred[,3]),
                           as.data.frame(pred2$pred[,3]))

setnames(train.predictions, c("pr_label0_ensemble", #"pr_label0_drf",
                              "pr_label1_ensemble", #"pr_label1_drf",
                              "pr_label2_ensemble"))#, "pr_label2_drf"))

test.predictions <- cbind( as.data.frame(pred0.test$pred[,3]),
                           as.data.frame(pred1.test$pred[,3]),
                           as.data.frame(pred2.test$pred[,3]))#,
                           #as.data.frame(pred0.test$basepred[,2]),
                           #as.data.frame(pred1.test$basepred[,2]),
                           #as.data.frame(pred2.test$basepred[,2]))

setnames(test.predictions, c("pr_label0_ensemble", #"pr_label0_drf",
                             "pr_label1_ensemble", #"pr_label1_drf",
                             "pr_label2_ensemble"))  #, "pr_label2_drf"))

train_level1_data_for_level2 <- cbind(train.n, train.predictions)
test_level1_data_for_level2  <- cbind(test.n, test.predictions)

saveRDS(train_level1_data_for_level2, "train_level1_data_for_level2.RDS")
saveRDS(test_level1_data_for_level2,  "test_level1_data_for_level2.RDS")

# Stage 2 Model (same as in tel.R)
trainHex <- as.h2o(train_level1_data_for_level2, destination_frame="train_tel.hex")
testHex  <- as.h2o(test_level1_data_for_level2,  destination_frame="test_tel.hex")

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

rfHex <- h2o.randomForest(x               = x_names,
                          y               = "fault_severity_factor",
                          training_frame  = trainHex,
                          model_id        = "telstra_drf.hex",
                          ntrees          = 1000,
                          sample_rate     = 0.99,
                          balance_classes = F,
                          nbins           = 100,
                          max_depth       = 15) #,
                          #nfold           = 5)
rfHex
h2o.mse(rfHex)
sqrt(h2o.mse(rfHex))
h2o.varimp(rfHex)
#a <- as.data.frame(h2o.varimp(rfHex))
#saveRDS(a, "a.RDS")
rm(stage1.drf.pred)

stage1.drf.pred <- as.data.frame(h2o.predict(rfHex, testHex))

# Submission --------------------------------------------------------------
# For classification:
rm(sub)
stage1.drf.pred$predict <- NULL
sub <- cbind(sample$id, stage1.drf.pred)
setnames(sub, colnames(sample))
write.csv(sub, "maxdepth15_nfold5_ntrees1000_vars530_samplerate99_binds100_drf_basepreds_and_level1sensembles.csv", row.names = F)

# Level 1
rm(sub)
sub <- cbind(sample$id, test.predictions)
setnames(sub, colnames(sample))
write.csv(sub, "level_0_drf_1perclass.csv", row.names = F) # .6 -- ugly
