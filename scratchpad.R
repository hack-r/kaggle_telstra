
rf.grid <- h2o.grid("randomForest",
                             x                  = x_names[!(x_names %in% c("pr_label0_drf",
                                                                           "pr_label1_drf",
                                                                           "pr_label2_drf",
                                                                           "pr_label0_ensemble",
                                                                           "pr_label1_ensemble",
                                                                           "pr_label2_ensemble"))],
                             y                  = "fault_severity_factor",
                             training_frame     = trainHex,
                             model_id           = "tel_drf_grid.hex",
                             ntrees             = 1000,
                             sample_rate        = 0.99,
                             #balance_classes    = F,
                             nbins              = 100,
                             #max_depth          = 9,
                             stopping_rounds    = 5,
                             stopping_metric    = "logloss",
                             stopping_tolerance = .001,
                             nfolds             = 10,
                             seed               = 2016,
                             hyper_params       = list(
                               max_depth   = c(21,22,23,24) #9,12,13,14,15,16,9,15,17, 19,
                               )
                    )

# Get grid summary
summary(rf.grid)
# Fetch grid models
model_ids <- rf.grid@model_ids
models    <- lapply(model_ids, function(id) { h2o.getModel(id)})

m <- as.character(model_ids)

h2o.mse(h2o.getModel("Grid_DRF_RTMP_sid_9b4a_236_model_R_1456676148949_5463_model_4"), xval = T)
h2o.mse(h2o.getModel("Grid_DRF_RTMP_sid_9b4a_236_model_R_1456676148949_5463_model_3"), xval = T)
h2o.mse(h2o.getModel("Grid_DRF_RTMP_sid_9b4a_236_model_R_1456676148949_5463_model_2"), xval = T)
h2o.mse(h2o.getModel("Grid_DRF_RTMP_sid_9b4a_236_model_R_1456676148949_5463_model_1"), xval = T)
