library(scipiper)
library(tidyverse)

#Check what water quality portal codes are already in the dataset
wqp_codes <- scmake('wqp_codes')
#Check the names for total suspended sediment
wqp_codes$characteristicName$tss

#Repull all wqp data
scmake('tasks_1_wqp.yml')

inventory <- feather('1_wqdata/out/wqp_inventory.feather') %>%
  as.tibble()
