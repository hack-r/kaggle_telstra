# Options -----------------------------------------------------------------
setwd("T:/RNA/Baltimore/Jason/ad_hoc/tel")

# Functions/Libraries -----------------------------------------------------
require(pacman)

pacman::p_load(adabag, bit64, cvAUC, data.table, dplyr,
               doParallel,ensembleBMA, extraTrees,
               glmulti, h2o, h2oEnsemble, Matrix,
               Metrics, mlbench, nnet, pls, qdap,
               rattle, reshape2, RRF, #RWeka,
               ROCR, sqldf, verification)

# Parallel Backend --------------------------------------------------------
cores_2_use <- detectCores() - 4
cl          <- makeCluster(cores_2_use, useXDR = F)
clusterSetRNGStream(cl, 9956)
registerDoParallel(cl, cores_2_use)


# Data --------------------------------------------------------------------
# Extract
sample   <- fread("sample_submission.csv")
train    <- fread("train.csv")
test     <- fread("test.csv")
severity <- fread("severity_type.csv")
resource <- fread("resource_type.csv")
event    <- fread("event_type.csv")
lf       <- fread("log_feature.csv")

# Transform
checksum.train <- nrow(train)
checksum.test  <- nrow(test)

train <- sqldf("select a.*, b.event_type from train a left join event b on a.id = b.id")
train <- sqldf("select a.*, b.resource_type from train a left join resource b on a.id = b.id")
train <- sqldf("select a.*, b.severity_type from train a left join severity b on a.id = b.id")

resource_type_1_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 1' group by id")
resource_type_2_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 2' group by id")
resource_type_3_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 3' group by id")
resource_type_4_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 4' group by id")
resource_type_5_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 5' group by id")
resource_type_6_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 6' group by id")
resource_type_7_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 7' group by id")
resource_type_8_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 8' group by id")
resource_type_9_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 9' group by id")
resource_type_10_cnt <- sqldf("select id, count(*) from train a where a.resource_type == 'resource_type 10' group by id")

event_type_1_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 1' group by id")
event_type_2_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 2' group by id")
event_type_3_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 3' group by id")
event_type_4_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 4' group by id")
event_type_5_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 5' group by id")
event_type_6_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 6' group by id")
event_type_7_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 7' group by id")
event_type_8_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 8' group by id")
event_type_9_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 9' group by id")
event_type_10_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 10' group by id")
event_type_11_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 11' group by id")
event_type_12_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 12' group by id")
event_type_13_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 13' group by id")
event_type_14_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 14' group by id")
event_type_15_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 15' group by id")
event_type_16_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 16' group by id")
event_type_17_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 17' group by id")
event_type_18_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 18' group by id")
event_type_19_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 19' group by id")
event_type_20_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 20' group by id")
event_type_21_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 21' group by id")
event_type_22_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 22' group by id")
event_type_23_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 23' group by id")
event_type_24_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 24' group by id")
event_type_25_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 25' group by id")
event_type_26_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 26' group by id")
event_type_27_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 27' group by id")
event_type_28_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 28' group by id")
event_type_29_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 29' group by id")
event_type_30_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 30' group by id")
event_type_31_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 31' group by id")
event_type_32_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 32' group by id")
event_type_33_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 33' group by id")
event_type_34_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 34' group by id")
event_type_35_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 35' group by id")
event_type_36_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 36' group by id")
event_type_37_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 37' group by id")
event_type_38_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 38' group by id")
event_type_39_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 39' group by id")
event_type_40_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 40' group by id")
event_type_41_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 41' group by id")
event_type_42_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 42' group by id")
event_type_43_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 43' group by id")
event_type_44_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 44' group by id")
event_type_45_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 45' group by id")
event_type_46_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 46' group by id")
event_type_47_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 47' group by id")
event_type_48_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 48' group by id")
event_type_49_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 49' group by id")
event_type_50_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 50' group by id")
event_type_51_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 51' group by id")
event_type_52_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 52' group by id")
event_type_53_cnt <- sqldf("select id, count(*) from train a where a.event_type == 'event_type 53' group by id")

severity_type_1_cnt <- sqldf("select id, count(*) from train a where a.severity_type == 'severity_type 1' group by id")
severity_type_2_cnt <- sqldf("select id, count(*) from train a where a.severity_type == 'severity_type 2' group by id")
severity_type_3_cnt <- sqldf("select id, count(*) from train a where a.severity_type == 'severity_type 3' group by id")
severity_type_4_cnt <- sqldf("select id, count(*) from train a where a.severity_type == 'severity_type 4' group by id")
severity_type_5_cnt <- sqldf("select id, count(*) from train a where a.severity_type == 'severity_type 5' group by id")

train    <- fread("train.csv")
test     <- fread("test.csv")


train <- sqldf("select a.*, b.`count(*)` as severity_type_1 from train a left join severity_type_1_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as severity_type_2 from train a left join severity_type_2_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as severity_type_3 from train a left join severity_type_3_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as severity_type_4 from train a left join severity_type_4_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as severity_type_5 from train a left join severity_type_5_cnt b on a.id = b.id")

train <- sqldf("select a.*, b.`count(*)` as resource_type_1 from train a left join resource_type_1_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_2 from train a left join resource_type_2_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_3 from train a left join resource_type_3_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_4 from train a left join resource_type_4_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_5 from train a left join resource_type_5_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_6 from train a left join resource_type_6_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_7 from train a left join resource_type_7_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_8 from train a left join resource_type_8_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_9 from train a left join resource_type_9_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as resource_type_10 from train a left join resource_type_10_cnt b on a.id = b.id")

train <- sqldf("select a.*, b.`count(*)` as event_type_1 from train a left join event_type_1_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_2 from train a left join event_type_2_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_3 from train a left join event_type_3_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_4 from train a left join event_type_4_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_5 from train a left join event_type_5_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_6 from train a left join event_type_6_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_7 from train a left join event_type_7_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_8 from train a left join event_type_8_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_9 from train a left join event_type_9_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_10 from train a left join event_type_10_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_11 from train a left join event_type_11_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_12 from train a left join event_type_12_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_13 from train a left join event_type_13_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_14 from train a left join event_type_14_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_15 from train a left join event_type_15_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_16 from train a left join event_type_16_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_17 from train a left join event_type_17_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_18 from train a left join event_type_18_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_19 from train a left join event_type_19_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_20 from train a left join event_type_20_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_21 from train a left join event_type_21_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_22 from train a left join event_type_22_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_23 from train a left join event_type_23_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_24 from train a left join event_type_24_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_25 from train a left join event_type_25_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_26 from train a left join event_type_26_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_27 from train a left join event_type_27_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_28 from train a left join event_type_28_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_29 from train a left join event_type_29_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_30 from train a left join event_type_30_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_31 from train a left join event_type_31_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_32 from train a left join event_type_32_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_33 from train a left join event_type_33_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_34 from train a left join event_type_34_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_35 from train a left join event_type_35_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_36 from train a left join event_type_36_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_37 from train a left join event_type_37_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_38 from train a left join event_type_38_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_39 from train a left join event_type_39_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_40 from train a left join event_type_40_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_41 from train a left join event_type_41_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_42 from train a left join event_type_42_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_43 from train a left join event_type_43_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_44 from train a left join event_type_44_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_45 from train a left join event_type_45_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_46 from train a left join event_type_46_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_47 from train a left join event_type_47_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_48 from train a left join event_type_48_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_49 from train a left join event_type_49_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_50 from train a left join event_type_50_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_51 from train a left join event_type_51_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_52 from train a left join event_type_52_cnt b on a.id = b.id")
train <- sqldf("select a.*, b.`count(*)` as event_type_53 from train a left join event_type_53_cnt b on a.id = b.id")

for(i in 4:71){
  train[is.na(train[,i]),i] <- 0
}

test  <- sqldf("select a.*, b.event_type from test  a left join event b on a.id = b.id")
test  <- sqldf("select a.*, b.resource_type from test  a left join resource b on a.id = b.id")
test  <- sqldf("select a.*, b.severity_type from test  a left join severity b on a.id = b.id")

resource_type_1_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 1' group by id")
resource_type_2_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 2' group by id")
resource_type_3_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 3' group by id")
resource_type_4_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 4' group by id")
resource_type_5_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 5' group by id")
resource_type_6_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 6' group by id")
resource_type_7_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 7' group by id")
resource_type_8_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 8' group by id")
resource_type_9_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 9' group by id")
resource_type_10_cnt <- sqldf("select id, count(*) from test a where a.resource_type == 'resource_type 10' group by id")

event_type_1_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 1' group by id")
event_type_2_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 2' group by id")
event_type_3_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 3' group by id")
event_type_4_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 4' group by id")
event_type_5_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 5' group by id")
event_type_6_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 6' group by id")
event_type_7_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 7' group by id")
event_type_8_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 8' group by id")
event_type_9_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 9' group by id")
event_type_10_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 10' group by id")
event_type_11_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 11' group by id")
event_type_12_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 12' group by id")
event_type_13_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 13' group by id")
event_type_14_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 14' group by id")
event_type_15_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 15' group by id")
event_type_16_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 16' group by id")
event_type_17_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 17' group by id")
event_type_18_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 18' group by id")
event_type_19_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 19' group by id")
event_type_20_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 20' group by id")
event_type_21_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 21' group by id")
event_type_22_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 22' group by id")
event_type_23_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 23' group by id")
event_type_24_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 24' group by id")
event_type_25_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 25' group by id")
event_type_26_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 26' group by id")
event_type_27_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 27' group by id")
event_type_28_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 28' group by id")
event_type_29_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 29' group by id")
event_type_30_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 30' group by id")
event_type_31_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 31' group by id")
event_type_32_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 32' group by id")
event_type_33_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 33' group by id")
event_type_34_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 34' group by id")
event_type_35_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 35' group by id")
event_type_36_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 36' group by id")
event_type_37_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 37' group by id")
event_type_38_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 38' group by id")
event_type_39_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 39' group by id")
event_type_40_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 40' group by id")
event_type_41_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 41' group by id")
event_type_42_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 42' group by id")
event_type_43_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 43' group by id")
event_type_44_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 44' group by id")
event_type_45_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 45' group by id")
event_type_46_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 46' group by id")
event_type_47_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 47' group by id")
event_type_48_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 48' group by id")
event_type_49_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 49' group by id")
event_type_50_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 50' group by id")
event_type_51_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 51' group by id")
event_type_52_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 52' group by id")
event_type_53_cnt <- sqldf("select id, count(*) from test a where a.event_type == 'event_type 53' group by id")

severity_type_1_cnt <- sqldf("select id, count(*) from test a where a.severity_type == 'severity_type 1' group by id")
severity_type_2_cnt <- sqldf("select id, count(*) from test a where a.severity_type == 'severity_type 2' group by id")
severity_type_3_cnt <- sqldf("select id, count(*) from test a where a.severity_type == 'severity_type 3' group by id")
severity_type_4_cnt <- sqldf("select id, count(*) from test a where a.severity_type == 'severity_type 4' group by id")
severity_type_5_cnt <- sqldf("select id, count(*) from test a where a.severity_type == 'severity_type 5' group by id")

test    <- fread("test.csv")

test <- sqldf("select a.*, b.`count(*)` as severity_type_1 from test a left join severity_type_1_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as severity_type_2 from test a left join severity_type_2_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as severity_type_3 from test a left join severity_type_3_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as severity_type_4 from test a left join severity_type_4_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as severity_type_5 from test a left join severity_type_5_cnt b on a.id = b.id")

test <- sqldf("select a.*, b.`count(*)` as resource_type_1 from test a left join resource_type_1_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_2 from test a left join resource_type_2_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_3 from test a left join resource_type_3_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_4 from test a left join resource_type_4_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_5 from test a left join resource_type_5_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_6 from test a left join resource_type_6_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_7 from test a left join resource_type_7_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_8 from test a left join resource_type_8_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_9 from test a left join resource_type_9_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as resource_type_10 from test a left join resource_type_10_cnt b on a.id = b.id")

test <- sqldf("select a.*, b.`count(*)` as event_type_1 from test a left join event_type_1_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_2 from test a left join event_type_2_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_3 from test a left join event_type_3_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_4 from test a left join event_type_4_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_5 from test a left join event_type_5_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_6 from test a left join event_type_6_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_7 from test a left join event_type_7_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_8 from test a left join event_type_8_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_9 from test a left join event_type_9_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_10 from test a left join event_type_10_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_11 from test a left join event_type_11_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_12 from test a left join event_type_12_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_13 from test a left join event_type_13_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_14 from test a left join event_type_14_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_15 from test a left join event_type_15_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_16 from test a left join event_type_16_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_17 from test a left join event_type_17_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_18 from test a left join event_type_18_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_19 from test a left join event_type_19_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_20 from test a left join event_type_20_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_21 from test a left join event_type_21_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_22 from test a left join event_type_22_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_23 from test a left join event_type_23_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_24 from test a left join event_type_24_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_25 from test a left join event_type_25_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_26 from test a left join event_type_26_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_27 from test a left join event_type_27_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_28 from test a left join event_type_28_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_29 from test a left join event_type_29_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_30 from test a left join event_type_30_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_31 from test a left join event_type_31_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_32 from test a left join event_type_32_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_33 from test a left join event_type_33_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_34 from test a left join event_type_34_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_35 from test a left join event_type_35_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_36 from test a left join event_type_36_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_37 from test a left join event_type_37_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_38 from test a left join event_type_38_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_39 from test a left join event_type_39_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_40 from test a left join event_type_40_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_41 from test a left join event_type_41_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_42 from test a left join event_type_42_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_43 from test a left join event_type_43_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_44 from test a left join event_type_44_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_45 from test a left join event_type_45_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_46 from test a left join event_type_46_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_47 from test a left join event_type_47_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_48 from test a left join event_type_48_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_49 from test a left join event_type_49_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_50 from test a left join event_type_50_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_51 from test a left join event_type_51_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_52 from test a left join event_type_52_cnt b on a.id = b.id")
test <- sqldf("select a.*, b.`count(*)` as event_type_53 from test a left join event_type_53_cnt b on a.id = b.id")

for(i in 3:70){
  test[is.na(test[,i]),i] <- 0
}

train$severity_sum <- train[,4] +train[,5] + train[,6] + train[,7] + train[,8]
train$resource_sum <- train[,9] +train[,10] + train[,11] + train[,12] + train[,13]+ train[,14]+ train[,15]+ train[,16]+ train[,17]+ train[,18]

train$event_sum <- 0
for(i in 19:71){
  train[,i] <- as.numeric(train[,i])
  train$event_sum <- train[, i] + train$event_sum
  #apply(train,1, function(x) x$event_sum <- x["event_sum"] + train[i])
}

train$severity_sum_ln <- log(train$severity_sum)
train$resource_sum_ln <- log(train$resource_sum)
train$event_sum_ln    <- log(train$event_sum)

train <- data.table(train)
for (j in 1:ncol(train)) set(train, which(is.infinite(train[[j]])), j, NA)

train$all_sum         <- train$severity_sum + train$resource_sum + train$event_sum
train$all_sum_ln      <- log(train$all_sum)

train$event_sq <- train$event_sum*train$event_sum

test$severity_sum <- test[,4] +test[,5] + test[,6] + test[,7] + test[,3]
test$resource_sum <- test[,9] +test[,10] + test[,11] + test[,12] + test[,13]+ test[,14]+ test[,15]+ test[,16]+ test[,17]+ test[,8]
test$event_sum <- 0
for(i in 18:70){
  test[,i] <- as.numeric(test[,i])
  test$event_sum <- test[, i] + test$event_sum
}

test$severity_sum_ln <- log(test$severity_sum)
test$resource_sum_ln <- log(test$resource_sum)
test$event_sum_ln    <- log(test$event_sum)

test$all_sum         <- test$severity_sum + test$resource_sum + test$event_sum
test$all_sum_ln      <- log(test$all_sum)
test <- data.table(test)
for (j in 1:ncol(test)) set(test, which(is.infinite(test[[j]])), j, NA)

test$event_sq <- test$event_sum*test$event_sum

# Adding Log Feature Data Post Hoc ----------------------------------------
lf.melt <- melt(lf, id.vars = c("id", "log_feature"))
lf.cast <- dcast(lf.melt, id ~ log_feature, value.var = "value")

for(i in 2:ncol(lf.cast)){
  lf.cast[is.na(lf.cast[,i]),i] <- 0
}

train <- sqldf("select a.*, b.* from train a left join 'lf.cast' b on a.id = b.id")
test  <- sqldf("select a.*, b.* from test  a left join 'lf.cast' b on a.id = b.id")

train$id <- NULL
test$id  <- NULL

# PCA ---------------------------------------------------------------------
x <- train.n
x <- x[,apply(x, 2, function(col) { length(unique(col)) > 1 })]
xx    <- x[,2:200]
#x.log <- log(xx[,1:100])
x.pca <- prcomp(xx[!(xx %in% c("location", "id"))],
                center = TRUE,
                scale. = TRUE,
                na.action=na.omit,
                tol = .25)

x.pca.pred <- predict(x.pca, newdata = train)
dim(x.pca.pred)
train <- cbind(train, x.pca.pred)

x.pca.pred <- predict(x.pca, newdata = test)
dim(x.pca.pred)
test <- cbind(test, x.pca.pred)

# Cluster Analysis -------------------------------------------------------
train$location.num <- gsub("location ","",train$location)
test$location.num  <- gsub("location ","",test$location)
train$location.num <- as.numeric(train$location.num)
test$location.num  <- as.numeric(test$location.num)

nums <- sapply(train, is.numeric)
colnames(train[!nums])

train.n <- train
test.n  <- test
train.n$location <- NULL
test.n$location  <- NULL

train.n[!is.finite(train.n),] <- NA
test.n[!is.finite(test.n)]    <- NA
require(RRF)
train.n <- na.roughfix(train.n)
test.n  <- na.roughfix(test.n)

train.n <- as.data.frame(train.n)
test.n  <- as.data.frame(test.n)
train.n$location <- NULL
test.n$location  <- NULL

# Determine number of clusters
wss <- (nrow(train.n)-1)*sum(apply(train.n[,2:74],2,var))
for (i in 2:74) wss[i] <- sum(kmeans(train.n[,2:74],
                                     centers=i)$withinss)
plot(1:74, wss, type="b", xlab="Number of Clusters",
     ylab="Within groups sum of squares")

# K-Means Cluster Analysis
# append cluster assignment
fit3    <- kmeans(train.n[2:74], 3)
train.n <- data.frame(train.n, fit3$cluster)
fit3    <- kmeans(test.n[1:73], 3)
test.n  <- data.frame(test.n, fit3$cluster)

fit20   <- kmeans(train.n[2:74], 20)
train.n <- data.frame(train.n, fit20$cluster)
fit20   <- kmeans(test.n[1:73], 20)
test.n  <- data.frame(test.n, fit20$cluster)

fit2a    <- kmeans(train.n[2:400], 2)
train.n  <- data.frame(train.n, fit2a$cluster)
fit2a    <- kmeans(test.n[1:399], 2)
test.n   <- data.frame(test.n, fit2a$cluster)

fit3a    <- kmeans(train.n[2:400], 3)
train.n  <- data.frame(train.n, fit3a$cluster)
fit3a    <- kmeans(test.n[1:399], 3)
test.n   <- data.frame(test.n, fit3a$cluster)

fit5a    <- kmeans(train.n[2:400], 5)
train.n  <- data.frame(train.n, fit5a$cluster)
fit5a    <- kmeans(test.n[1:399], 5)
test.n   <- data.frame(test.n, fit5a$cluster)


## New features... ##
# Key feature, PCs variations
train.n$f203_gr_0 <- 0
train.n$f203_gr_0[train.n$`feature 203` > 0] <- 1
train.n$f203_gr_20 <- 0
train.n$f203_gr_20[train.n$`feature 203` > 20] <- 1
train.n$f203_sq <- train.n$`feature 203` * train.n$`feature 203`

train.n$f82_gr_0 <- 0
train.n$f82_gr_0[train.n$`feature 82` > 0] <- 1
train.n$f82_gr_5 <- 0
train.n$f82_gr_5[train.n$`feature 82` > 5] <- 1
train.n$f82_gr_20 <- 0
train.n$f82_gr_20[train.n$`feature 82` > 20] <- 1
train.n$f82_sq <- train.n$`feature 82` * train.n$`feature 82`

train.n$pc112_gr_0 <- 0
train.n$pc112_gr_0[train.n$PC112 > 0] <- 1
train.n$pc112_gr_20 <- 0
train.n$pc112_gr_20[abs(train.n$PC112) > 20] <- 1
train.n$pc112_sq <- train.n$PC112 * train.n$PC112

train.n$pc5_gr_0 <- 0
train.n$pc5_gr_0[train.n$PC5 > 0] <- 1
train.n$pc5_gr_20 <- 0
train.n$pc5_gr_20[abs(train.n$PC5) > 20] <- 1
train.n$pc5_sq <- train.n$PC5 * train.n$PC5

test.n$f203_gr_0 <- 0
test.n$f203_gr_0[test.n$`feature 203` > 0] <- 1
test.n$f203_gr_20 <- 0
test.n$f203_gr_20[test.n$`feature 203` > 20] <- 1
test.n$f203_sq <- test.n$`feature 203` * test.n$`feature 203`

test.n$f82_gr_0 <- 0
test.n$f82_gr_0[test.n$`feature 82` > 0] <- 1
test.n$f82_gr_5 <- 0
test.n$f82_gr_5[test.n$`feature 82` > 5] <- 1
test.n$f82_gr_20 <- 0
test.n$f82_gr_20[test.n$`feature 82` > 20] <- 1
test.n$f82_sq <- test.n$`feature 82` * test.n$`feature 82`

test.n$pc112_gr_0 <- 0
test.n$pc112_gr_0[test.n$PC112 > 0] <- 1
test.n$pc112_gr_20 <- 0
test.n$pc112_gr_20[abs(test.n$PC112) > 20] <- 1
test.n$pc112_sq <- test.n$PC112 * test.n$PC112

test.n$pc5_gr_0 <- 0
test.n$pc5_gr_0[test.n$PC5 > 0] <- 1
test.n$pc5_gr_20 <- 0
test.n$pc5_gr_20[abs(test.n$PC5) > 20] <- 1
test.n$pc5_sq <- test.n$PC5 * test.n$PC5

# location-based
train.n$location.num.sq <- train.n$location.num * train.n$location.num
train.n$location.num.ln <- log(train.n$location.num)
train.n$location.top    <- 0
train.n$location.top[train.n$location.num %in% c(821,1107,734,1008,126)] <- 1
train.n$loc_num_f203_cor <- cor(train.n$location.num, train.n$`feature 203`)

test.n$location.num.sq <- test.n$location.num * test.n$location.num
test.n$location.num.ln <- log(test.n$location.num)
test.n$location.top    <- 0
test.n$location.top[test.n$location.num %in% c(821,1107,734,1008,126)] <- 1
test.n$loc_num_f203_cor <- cor(test.n$location.num, test.n$`feature 203`)

# More PC transformations
train.n$PC1_112                   <- train.n$PC1 + train.n$PC112
train.n$PC1_gr_0                  <- 0
train.n$PC1_gr_0[train.n$PC1 > 0] <- 1
train.n$PC112_sine                <- asin(train.n$PC112/100)
train.n$location.top.pc112        <- train.n$location.top*train.n$PC112

test.n$PC1_112                   <- test.n$PC1 + test.n$PC112
test.n$PC1_gr_0                  <- 0
test.n$PC1_gr_0[test.n$PC1 > 0]  <- 1
test.n$PC112_sine                <- asin(test.n$PC112/100)
test.n$location.top.pc112        <- test.n$location.top*test.n$PC112

test.n$PC112_sine <- na.roughfix(test.n$PC112_sine)

# try some more stuff
train.n$sum_strong_feats <- train.n$`feature 203` + train.n$`feature 82` + train.n$PC112 + train.n$PC17 + train.n$PC5
test.n$sum_strong_feats  <- test.n$`feature 203`  + test.n$`feature 82`  + test.n$PC112  + test.n$PC17  + test.n$PC5

saveRDS(train.n, "train.n.RDS")
saveRDS(test.n, "test.n.RDS")

# Hunt for the magic feature ----------------------------------------------
train.n$feature.203_0_1 <-0
train.n$feature.203_0_1[train.n$feature.203 < 1] <- 1

train.n$feature.203_1_2 <-0
train.n$feature.203_1_2[train.n$feature.203 == 2 | train.n$feature.203 == 1] <- 1

train.n$location <- as.factor(train$location)
train.n$two_feat <- train.n$feature.203 + train.n$feature.82

test.n$feature.203_0_1 <-0
test.n$feature.203_0_1[test.n$feature.203 < 1] <- 1

test.n$feature.203_1_2 <-0
test.n$feature.203_1_2[test.n$feature.203 == 2 | test.n$feature.203 == 1] <- 1

test.n$location <- as.factor(test$location)
test.n$two_feat <- test.n$feature.203 + test.n$feature.82

# NEW
train.n$severity_type_2_gr_4 <- 0
train.n$severity_type_2_gr_4[train.n$severity_type_2 > 4] <- 1

train.n$severity_type_3_gr_0 <- 0
train.n$severity_type_3_gr_0[train.n$severity_type_3 > 4] <- 1

train.n$severity_type_4_gr_0 <- 0
train.n$severity_type_4_gr_0[train.n$severity_type_4 > 0] <- 1

train.n$severity_type_5_gr_0 <- 0
train.n$severity_type_5_gr_0[train.n$severity_type_5 > 0] <- 1

test.n$severity_type_2_gr_4 <- 0
test.n$severity_type_2_gr_4[test.n$severity_type_2 > 4] <- 1

test.n$severity_type_3_gr_0 <- 0
test.n$severity_type_3_gr_0[test.n$severity_type_3 > 4] <- 1

test.n$severity_type_4_gr_0 <- 0
test.n$severity_type_4_gr_0[test.n$severity_type_4 > 0] <- 1

test.n$severity_type_5_gr_0 <- 0
test.n$severity_type_5_gr_0[test.n$severity_type_5 > 0] <- 1

#NEWER
train.n$event_type_2_gr_1 <- 0
train.n$event_type_2_gr_1[train.n$event_type_2 >1] <- 1

train.n$event_type_3_gr_1 <- 0
train.n$event_type_3_gr_1[train.n$event_type_3 >1] <- 1

train.n$event_type_15_gr_2 <- 0
train.n$event_type_15_gr_2[train.n$event_type_15 >2] <- 1

train.n$event_type_24_gr_2 <- 0
train.n$event_type_24_gr_2[train.n$event_type_24 > 2] <- 1

train.n$event_type_21_gr_1 <- 0
train.n$event_type_21_gr_1[train.n$event_type_21 >1] <- 1

train.n$event_type_21_17 <- train.n$event_type_21 + train.n$event_type_17

train.n$PC1_5 <- train.n$PC1 + train.n$PC5

train.n$feature.203_cb <- train.n$feature.203*train.n$feature.203*train.n$feature.203

test.n$event_type_2_gr_1 <- 0
test.n$event_type_2_gr_1[test.n$event_type_2 >1] <- 1

test.n$event_type_3_gr_1 <- 0
test.n$event_type_3_gr_1[test.n$event_type_3 >1] <- 1

test.n$event_type_15_gr_2 <- 0
test.n$event_type_15_gr_2[test.n$event_type_15 >2] <- 1

test.n$event_type_24_gr_2 <- 0
test.n$event_type_24_gr_2[test.n$event_type_24 > 2] <- 1

test.n$event_type_21_gr_1 <- 0
test.n$event_type_21_gr_1[test.n$event_type_21 >1] <- 1

test.n$event_type_21_17 <- test.n$event_type_21 + test.n$event_type_17

test.n$PC1_5 <- test.n$PC1 + test.n$PC5

test.n$feature.203_cb <- test.n$feature.203*test.n$feature.203*test.n$feature.203

# Drop constant columns
# const <- c("feature.184",   "feature.382", "feature.381",      "feature.263",   "feature.185",
#            "event_type_52", "feature.269", "feature.100",      "feature.342",   "feature.386",
#            "feature.264",   "feature.385", "feature.102",      "feature.300",   "feature.344",
#            "feature.343",   "feature.93",  "feature.14",       "event_type_4",  "feature.294",
#            "feature.252",   "feature.296", "feature.19",       "feature.372",   "loc_num_f203_cor",
#            "feature.214",   "feature.379", "feature.138",      "event_type_16", "event_type_17",
#            "feature.129",   "feature.23",  "feature.203_1_2",  "feature.282",   "feature.281",
#            "feature.126",   "feature.324", "feature.246",      "feature.326",   "feature.287",
#            "feature.121",   "feature.286", "feature.321",      "feature.72",    "feature.6",
#            "feature.4",     "feature.119", "feature.3",        "feature.32",    "feature.31",
#            "feature.36",    "feature.34",  "feature.151",      "feature.272",   "feature.271",
#            "feature.351",   "feature.270", "feature.356",      "feature.116",   "feature.352",
#            "feature.355",   "event_type_33")
#
# for(i in unique(colnames(train.n))){
#   if(var(train.n[,i]) == 0)
#   print(colnames(train.n[,i]))
# }

# NEWEST
for(i in unique(train.n$severity_type_1)){
  train.n$f203_var_st1[train.n$severity_type_1 == i] <- var(train.n$feature.203[train.n$severity_type_1 == i])
}
for(i in unique(train.n$severity_type_2)){
  train.n$f203_var_st2[train.n$severity_type_2 == i] <- var(train.n$feature.203[train.n$severity_type_2 == i])
}
for(i in unique(train.n$severity_type_3)){
  train.n$f203_var_st3[train.n$severity_type_3 == i] <- var(train.n$feature.203[train.n$severity_type_3 == i])
}
for(i in unique(train.n$severity_type_4)){
  train.n$f203_var_st4[train.n$severity_type_4 == i] <- var(train.n$feature.203[train.n$severity_type_4 == i])
}
for(i in unique(train.n$severity_type_5)){
  train.n$f203_var_st5[train.n$severity_type_5 == i] <- var(train.n$feature.203[train.n$severity_type_5 == i])
}

for(i in unique(train.n$severity_type_1)){
  train.n$f82_var_st1[train.n$severity_type_1 == i] <- var(train.n$feature.82[train.n$severity_type_1 == i])
}
for(i in unique(train.n$severity_type_2)){
  train.n$f82_var_st2[train.n$severity_type_2 == i] <- var(train.n$feature.82[train.n$severity_type_2 == i])
}
for(i in unique(train.n$severity_type_3)){
  train.n$f82_var_st3[train.n$severity_type_3 == i] <- var(train.n$feature.82[train.n$severity_type_3 == i])
}
for(i in unique(train.n$severity_type_4)){
  train.n$f82_var_st4[train.n$severity_type_4 == i] <- var(train.n$feature.82[train.n$severity_type_4 == i])
}
for(i in unique(train.n$severity_type_5)){
  train.n$f82_var_st5[train.n$severity_type_5 == i] <- var(train.n$feature.82[train.n$severity_type_5 == i])
}

for(i in unique(train.n$severity_type_1)){
  train.n$f203_mean_st1[train.n$severity_type_1 == i] <- mean(train.n$feature.203[train.n$severity_type_1 == i])
}
for(i in unique(train.n$severity_type_2)){
  train.n$f203_mean_st2[train.n$severity_type_2 == i] <- mean(train.n$feature.203[train.n$severity_type_2 == i])
}
for(i in unique(train.n$severity_type_3)){
  train.n$f203_mean_st3[train.n$severity_type_3 == i] <- mean(train.n$feature.203[train.n$severity_type_3 == i])
}
for(i in unique(train.n$severity_type_4)){
  train.n$f203_mean_st4[train.n$severity_type_4 == i] <- mean(train.n$feature.203[train.n$severity_type_4 == i])
}
for(i in unique(train.n$severity_type_5)){
  train.n$f203_mean_st5[train.n$severity_type_5 == i] <- mean(train.n$feature.203[train.n$severity_type_5 == i])
}

for(i in unique(train.n$severity_type_1)){
  train.n$f82_mean_st1[train.n$severity_type_1 == i] <- mean(train.n$feature.82[train.n$severity_type_1 == i])
}
for(i in unique(train.n$severity_type_2)){
  train.n$f82_mean_st2[train.n$severity_type_2 == i] <- mean(train.n$feature.82[train.n$severity_type_2 == i])
}
for(i in unique(train.n$severity_type_3)){
  train.n$f82_mean_st3[train.n$severity_type_3 == i] <- mean(train.n$feature.82[train.n$severity_type_3 == i])
}
for(i in unique(train.n$severity_type_4)){
  train.n$f82_mean_st4[train.n$severity_type_4 == i] <- mean(train.n$feature.82[train.n$severity_type_4 == i])
}
for(i in unique(train.n$severity_type_5)){
  train.n$f82_mean_st5[train.n$severity_type_5 == i] <- mean(train.n$feature.82[train.n$severity_type_5 == i])
}

for(i in unique(test.n$severity_type_1)){
  test.n$f203_var_st1[test.n$severity_type_1 == i] <- var(test.n$feature.203[test.n$severity_type_1 == i])
}
for(i in unique(test.n$severity_type_2)){
  test.n$f203_var_st2[test.n$severity_type_2 == i] <- var(test.n$feature.203[test.n$severity_type_2 == i])
}
for(i in unique(test.n$severity_type_3)){
  test.n$f203_var_st3[test.n$severity_type_3 == i] <- var(test.n$feature.203[test.n$severity_type_3 == i])
}
for(i in unique(test.n$severity_type_4)){
  test.n$f203_var_st4[test.n$severity_type_4 == i] <- var(test.n$feature.203[test.n$severity_type_4 == i])
}
for(i in unique(test.n$severity_type_5)){
  test.n$f203_var_st5[test.n$severity_type_5 == i] <- var(test.n$feature.203[test.n$severity_type_5 == i])
}

for(i in unique(test.n$severity_type_1)){
  test.n$f82_var_st1[test.n$severity_type_1 == i] <- var(test.n$feature.82[test.n$severity_type_1 == i])
}
for(i in unique(test.n$severity_type_2)){
  test.n$f82_var_st2[test.n$severity_type_2 == i] <- var(test.n$feature.82[test.n$severity_type_2 == i])
}
for(i in unique(test.n$severity_type_3)){
  test.n$f82_var_st3[test.n$severity_type_3 == i] <- var(test.n$feature.82[test.n$severity_type_3 == i])
}
for(i in unique(test.n$severity_type_4)){
  test.n$f82_var_st4[test.n$severity_type_4 == i] <- var(test.n$feature.82[test.n$severity_type_4 == i])
}
for(i in unique(test.n$severity_type_5)){
  test.n$f82_var_st5[test.n$severity_type_5 == i] <- var(test.n$feature.82[test.n$severity_type_5 == i])
}

for(i in unique(test.n$severity_type_1)){
  test.n$f203_mean_st1[test.n$severity_type_1 == i] <- mean(test.n$feature.203[test.n$severity_type_1 == i])
}
for(i in unique(test.n$severity_type_2)){
  test.n$f203_mean_st2[test.n$severity_type_2 == i] <- mean(test.n$feature.203[test.n$severity_type_2 == i])
}
for(i in unique(test.n$severity_type_3)){
  test.n$f203_mean_st3[test.n$severity_type_3 == i] <- mean(test.n$feature.203[test.n$severity_type_3 == i])
}
for(i in unique(test.n$severity_type_4)){
  test.n$f203_mean_st4[test.n$severity_type_4 == i] <- mean(test.n$feature.203[test.n$severity_type_4 == i])
}
for(i in unique(test.n$severity_type_5)){
  test.n$f203_mean_st5[test.n$severity_type_5 == i] <- mean(test.n$feature.203[test.n$severity_type_5 == i])
}

for(i in unique(test.n$severity_type_1)){
  test.n$f82_mean_st1[test.n$severity_type_1 == i] <- mean(test.n$feature.82[test.n$severity_type_1 == i])
}
for(i in unique(test.n$severity_type_2)){
  test.n$f82_mean_st2[test.n$severity_type_2 == i] <- mean(test.n$feature.82[test.n$severity_type_2 == i])
}
for(i in unique(test.n$severity_type_3)){
  test.n$f82_mean_st3[test.n$severity_type_3 == i] <- mean(test.n$feature.82[test.n$severity_type_3 == i])
}
for(i in unique(test.n$severity_type_4)){
  test.n$f82_mean_st4[test.n$severity_type_4 == i] <- mean(test.n$feature.82[test.n$severity_type_4 == i])
}
for(i in unique(test.n$severity_type_5)){
  test.n$f82_mean_st5[test.n$severity_type_5 == i] <- mean(test.n$feature.82[test.n$severity_type_5 == i])
}

#moar
for(i in unique(train.n$location)){
  train.n$f203_mean_loc[train.n$location == i] <- mean(train.n$feature.203[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f203_mean_loc[test.n$location == i] <- mean(test.n$feature.203[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f203_var_loc[train.n$location == i] <- var(train.n$feature.203[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f203_var_loc[test.n$location == i] <- var(test.n$feature.203[test.n$location == i])
}

#even moar
for(i in unique(train.n$location)){
  train.n$f82_mean_loc[train.n$location == i] <- mean(train.n$feature.82[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f82_mean_loc[test.n$location == i] <- mean(test.n$feature.82[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f82_var_loc[train.n$location == i] <- var(train.n$feature.82[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f82_var_loc[test.n$location == i] <- var(test.n$feature.82[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f82_max_loc[train.n$location == i] <- max(train.n$feature.82[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f82_max_loc[test.n$location == i] <- max(test.n$feature.82[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f82_min_loc[train.n$location == i] <- min(train.n$feature.82[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f82_min_loc[test.n$location == i] <- min(test.n$feature.82[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f203_max_loc[train.n$location == i] <- max(train.n$feature.203[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f203_max_loc[test.n$location == i] <- max(test.n$feature.203[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$f203_min_loc[train.n$location == i] <- min(train.n$feature.203[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$f203_min_loc[test.n$location == i] <- min(test.n$feature.203[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC1_mean_loc[train.n$location == i] <- mean(train.n$PC1[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC1_mean_loc[test.n$location == i] <- mean(test.n$PC1[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC1_var_loc[train.n$location == i] <- var(train.n$PC1[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC1_var_loc[test.n$location == i] <- var(test.n$PC1[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC1_max_loc[train.n$location == i] <- max(train.n$PC1[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC1_max_loc[test.n$location == i] <- max(test.n$PC1[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC1_min_loc[train.n$location == i] <- min(train.n$PC1[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC1_min_loc[test.n$location == i] <- min(test.n$PC1[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC2_max_loc[train.n$location == i] <- max(train.n$PC2[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC2_max_loc[test.n$location == i] <- max(test.n$PC2[test.n$location == i])
}
for(i in unique(train.n$location)){
  train.n$PC2_min_loc[train.n$location == i] <- min(train.n$PC2[train.n$location == i])
}

for(i in unique(test.n$location)){
  test.n$PC2_min_loc[test.n$location == i] <- min(test.n$PC2[test.n$location == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC1_meanfit20[train.n$fit20.factor == i] <- mean(train.n$PC1[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC1_meanfit20[test.n$fit20.factor == i] <- mean(test.n$PC1[test.n$fit20.factor == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC1_varfit20[train.n$fit20.factor == i] <- var(train.n$PC1[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC1_varfit20[test.n$fit20.factor == i] <- var(test.n$PC1[test.n$fit20.factor == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC1_maxfit20[train.n$fit20.factor == i] <- max(train.n$PC1[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC1_maxfit20[test.n$fit20.factor == i] <- max(test.n$PC1[test.n$fit20.factor == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC1_minfit20[train.n$fit20.factor == i] <- min(train.n$PC1[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC1_minfit20[test.n$fit20.factor == i] <- min(test.n$PC1[test.n$fit20.factor == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC2_maxfit20[train.n$fit20.factor == i] <- max(train.n$PC2[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC2_maxfit20[test.n$fit20.factor == i] <- max(test.n$PC2[test.n$fit20.factor == i])
}
for(i in unique(train.n$fit20.factor)){
  train.n$PC2_minfit20[train.n$fit20.factor == i] <- min(train.n$PC2[train.n$fit20.factor == i])
}

for(i in unique(test.n$fit20.factor)){
  test.n$PC2_minfit20[test.n$fit20.factor == i] <- min(test.n$PC2[test.n$fit20.factor == i])
}


# Model -------------------------------------------------------------------
h2o.init(nthreads=-1)

trainHex <- as.h2o(train.n, destination_frame="train_tel.hex")
testHex  <- as.h2o(test.n,  destination_frame="test_tel.hex")

trainHex[,"fault_severity_factor"] <- as.factor(trainHex[,"fault_severity"])

trainHex[,"location"]    <- as.factor(trainHex[,"location"])
trainHex[,"fit3_factor"] <- as.factor(trainHex[,"fit3.cluster"])
trainHex[,"fit20_factor"] <- as.factor(trainHex[,"fit20.cluster"])
trainHex[,"fit2a_factor"] <- as.factor(trainHex[,"fit2a.cluster"])
trainHex[,"fit3a_factor"] <- as.factor(trainHex[,"fit3a.cluster"])
trainHex[,"fit5a_factor"] <- as.factor(trainHex[,"fit5a.cluster"])

testHex[,"location"]    <- as.factor(testHex[,"location"])
testHex[,"fit3_factor"] <- as.factor(testHex[,"fit3.cluster"])
testHex[,"fit20_factor"] <- as.factor(testHex[,"fit20.cluster"])
testHex[,"fit2a_factor"] <- as.factor(testHex[,"fit2a.cluster"])
testHex[,"fit3a_factor"] <- as.factor(testHex[,"fit3a.cluster"])
testHex[,"fit5a_factor"] <- as.factor(testHex[,"fit5a.cluster"])

x_names <- colnames(testHex)
x_names <- x_names[!(x_names == "id")]
#x_names <- x_names[!(x_names %in% a$variable[a$percentage < .0058])]
length(x_names)

rfHex <- h2o.randomForest(x               = x_names[!(x_names %in% c("pr_label0_drf",
                                                                     "pr_label1_drf",
                                                                     "pr_label2_drf",
                                                                     "pr_label0_ensemble",
                                                                     "pr_label1_ensemble",
                                                                     "pr_label2_ensemble"))],
                          y               = "fault_severity_factor",
                          training_frame  = trainHex,
                          model_id        = "telstra_drf.hex",
                          ntrees          = 1500,
                          sample_rate     = 0.99,
                          balance_classes = F,
                          nbins           = 100,
                          max_depth       = 17)#,
                          #nfold           = 2)
rfHex
h2o.mse(rfHex)
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
#write.csv(sub, "final_day_sub1_530_feats_1500tree_17_deep.csv", row.names = F)
#write.csv(sub, "final_day_sub2_530_feats_1500tree_21_deep.csv", row.names = F)
write.csv(sub, "final_day_sub3_new_feats_1500tree_17_deep.csv", row.names = F)
write.csv(sub, "final_day_sub4_new_feats_1500tree_21_deep.csv", row.names = F)
write.csv(sub, "final_day_sub5_new_feats_1500tree_20_deep_ensemble.csv", row.names = F)

# For regression:
sub <- sample
sub$predict_1 <- 0
sub$predict_0[stage1.drf.pred < .5] <- 1
sub$predict_1[stage1.drf.pred < 1.5 & stage1.drf.pred > .5] <- 1
sub$predict_2[stage1.drf.pred > 1.5] <- 1
summary(sub)
summary(train$fault_severity)
write.csv(sub, "tel_sub.csv", row.names = F)
