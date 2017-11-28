# this may be a stop-gap file but permits preliminary analyses by pulling
# chlorophyll and sediment data for the 50 rivers matt has identified that are
# sufficiently wide, have metabolism data from powell center analysis, etc.

library(dataRetrieval)
library(dplyr)
library(ggplot2)

# read in sites from Matt
metsites_varname <- load('1_wqdata/in/satsites.RData')
metsites <- get(metsites_varname)

# choose appropriate-sounding parameter codes
all_pcodes <- dataRetrieval::parameterCdFile
chl_pcodes <- filter(all_pcodes, (grepl("chlorophyll", tolower(parameter_nm)) | grepl("chlorophyll", tolower(srsname))) & !grepl("regression", tolower(parameter_nm)))
sed_pcodes <- filter(all_pcodes, parameter_group_nm=='Sediment' & grepl("suspended sediment concentration", tolower(parameter_nm)) & !grepl("regression|millimeters|diameter", tolower(parameter_nm)))

#### chl data ####

chl_inventory <- dataRetrieval::whatNWISdata(siteNumbers=metsites, parameterCd=chl_pcodes$parameter_cd) # startDate gets ignored
ggplot(chl_inventory, aes(x=parm_cd, y=count_nu, color=site_no)) + geom_point() + scale_y_log10() + theme_bw() + theme(axis.text.x=element_text(angle=90))
chl_inventory %>% filter(is.na(stat_cd)) %>% pull(count_nu) %>% sum()
chl_inventory %>% filter(is.na(stat_cd)) %>% group_by(site_no) %>% summarize(num_params=length(unique(parm_cd)), num_samples=sum(count_nu))
chl_inventory %>% filter(is.na(stat_cd)) %>% group_by(site_no) %>% summarize(num_params=length(unique(parm_cd)), num_samples=sum(count_nu)) %>% pull(num_samples) %>% sum
chl_inventory %>% filter(is.na(stat_cd)) %>% count(data_type_cd)

# wq is the only data_type from the inventory above
system.time({
  chl_qw_all <- dataRetrieval::readNWISqw(siteNumbers=metsites, parameterCd=chl_pcodes$parameter_cd)
}) # 4 secs, 1790 rows, 35 cols
# inspect the complete output and check that it matches the inventory
ggplot(chl_qw_all, aes(x=startDateTime, y=site_no, color=result_va)) + geom_point()
chl_qw_inv <- chl_qw_all %>% group_by(site_no, parm_cd) %>% count()
chl_inv_merged <- select(filter(chl_inventory, is.na(stat_cd)), site_no, parm_cd, count_nu) %>% left_join(chl_qw_inv, by=c('site_no', 'parm_cd')) %>% rename(inventory=count_nu, final_count=n)
discrepancies <- chl_inv_merged %>% filter(is.na(final_count) | final_count != inventory)
stopifnot(sum(filter(chl_inventory, is.na(stat_cd))$count_nu) == sum(chl_qw_inv$n))

# now filter to just those dates that might help us
chl_qw <- filter(chl_qw_all, lubridate::ymd(sample_dt) >= lubridate::ymd("1984-10-01"))
ggplot(chl_qw, aes(x=startDateTime, y=site_no, color=result_va)) + geom_point()

# save the qw data
feather::write_feather(chl_qw, "1_wqdata/out/nwis_metab_chl.feather")

# daily values - only one site, all for "Biological Chlorophyll, total, water, fluorometric, 650-700 nanometers, in situ sensor, micrograms per liter"
system.time({
  chl_dv <- dataRetrieval::readNWISdata(service="dv", siteNumbers=metsites, parameterCd=chl_pcodes$parameter_cd, startDate = "1984-10-01")
}) # 0.7 secs, 640 rows, 10 cols, all for 1 site (stat code 00003 is daily mean; see https://help.waterdata.usgs.gov/stat_code)
ggplot(chl_dv, aes(x=dateTime, y=site_no, color=X_62361_00003)) + geom_point()
feather::write_feather(chl_dv, "1_wqdata/out/nwis_metab_chl_dv.feather")


#### sed data ####

sed_inventory <- dataRetrieval::whatNWISdata(siteNumbers=metsites, parameterCd=sed_pcodes$parameter_cd) # startDate gets ignored
ggplot(sed_inventory, aes(x=parm_cd, y=count_nu, color=site_no)) + geom_point() + scale_y_log10() + theme_bw() + theme(axis.text.x=element_text(angle=90))
sed_inventory %>% filter(is.na(stat_cd)) %>% pull(count_nu) %>% sum()
sed_inventory %>% filter(is.na(stat_cd)) %>% group_by(site_no) %>% summarize(num_params=length(unique(parm_cd)), num_samples=sum(count_nu))
sed_inventory %>% filter(is.na(stat_cd)) %>% group_by(site_no) %>% summarize(num_params=length(unique(parm_cd)), num_samples=sum(count_nu)) %>% pull(num_samples) %>% sum
sed_inventory %>% filter(is.na(stat_cd)) %>% count(data_type_cd)


system.time({
  sed_qw_all <- dataRetrieval::readNWISqw(siteNumbers=metsites, parameterCd=sed_pcodes$parameter_cd)
}) # 22 secs
# inspect the complete output and check that it matches the inventory
ggplot(sed_qw_all, aes(x=startDateTime, y=site_no, color=result_va)) + geom_point()
sed_qw_inv <- sed_qw_all %>% group_by(site_no, parm_cd) %>% count()
sed_inv_merged <- select(filter(sed_inventory, is.na(stat_cd)), site_no, parm_cd, count_nu) %>% left_join(sed_qw_inv, by=c('site_no', 'parm_cd')) %>% rename(inventory=count_nu, final_count=n)
discrepancies <- sed_inv_merged %>% filter(is.na(final_count) | final_count != inventory)
if(sum(filter(sed_inventory, is.na(stat_cd))$count_nu) != sum(sed_qw_inv$n)) warning("inventory and returned samples don't quite match")
# but it's really not so bad, only 2 sites and only a fraction of each record missing:
discrepancies %>% group_by(site_no, parm_cd) %>% summarize(inventory=sum(inventory), final_count=unique(final_count)) %>% filter(inventory != final_count)

# now filter to just those dates that might help us
sed_qw <- filter(sed_qw_all, lubridate::ymd(sample_dt) >= lubridate::ymd("1984-10-01"))
ggplot(sed_qw, aes(x=startDateTime, y=site_no, color=result_va)) + geom_point()

# save the qw data
feather::write_feather(sed_qw, "1_wqdata/out/nwis_metab_sed.feather")
