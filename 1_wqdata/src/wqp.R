# we've been asked to please NOT use a cluster because a postdoc took down the
# entire WQP a couple of years ago by doing that, and 12 simultaneous requests
# would be way too many. Alternative ways we can speed up the pull:
# * subset spatially rather than temporally - loop over state/HUC rather than years
# * probably fastest to pull all variables at once
# * probably faster to request whole states at once rather than giving explicit site lists


# acquire a data frame of site x constituent information, with counts of
# observations per site-constituent combination and all the site metadata that
# looks useful
inventory_wqp <- function(ind_file, wqp_state_codes, wqp_states_yml, wqp_codes_yml, wq_dates_yml) {
  # read states list and convert to FIPS list
  wqp_states <- yaml::yaml.load_file(wqp_states_yml)
  state_codes <- filter(wqp_state_codes, name %in% wqp_states) %>% pull(value)
  
  # read dates
  wq_dates <- yaml::yaml.load_file(wq_dates_yml)
  
  # read WQ codes and identify available constituent sets
  wqp_codes <- yaml::yaml.load_file(wqp_codes_yml)
  constituents <- names(wqp_codes$characteristicName)
  
  # prepare the args to whatWQPdata. all arguments will be the same every time
  # except characteristicName, which we'll loop through to get separate counts
  # for each
  wqp_args <- list(
    statecode=state_codes,
    siteType=wqp_codes$siteType,
    characteristicName=NA, # to be updated each time through loop
    sampleMedia=wqp_codes$sampleMedia,
    startDateLo=wq_dates$startDate,
    startDateHi=wq_dates$endDate)
  
  # loop over the constituents, getting rows for each
  sample_time <- system.time({
    samples <- bind_rows(lapply(constituents, function(constituent) {
      message(Sys.time(), ': getting inventory for ', constituent)
      wqp_args$characteristicName <- wqp_codes$characteristicName[[constituent]]
      tryCatch({
        wqp_wdat <- do.call(whatWQPdata, wqp_args)
        mutate(wqp_wdat, constituent=constituent)
      }, error=function(e) {
        # keep going IFF the only error was that there weren't any matching sites
        if(grepl('arguments imply differing number of rows', e$message)) {
          NULL
        } else {
          stop(e)
        }
      })
    }))
  })
  message(sprintf('sample inventory complete, required %0.0f seconds', sample_time[['elapsed']]))
  
  # get additional site information
  message(Sys.time(), ': getting additional site data')
  site_time <- system.time({
    wqp_site_args <- wqp_args[names(wqp_args) != 'characteristicName']
    sites <- do.call(whatWQPsites, wqp_site_args)
  })
  message(sprintf('site inventory complete, required %0.0f seconds', site_time[['elapsed']]))
  
  # merge constituent info with site info
  wqp_info <- left_join(
    inventory %>%
      select(Constituent=constituent, MonitoringLocationIdentifier, resultCount,
             MonitoringLocationName, MonitoringLocationTypeName, ResolvedMonitoringLocationTypeName),
    sites %>%
      select(MonitoringLocationIdentifier, LatitudeMeasure, LongitudeMeasure, HorizontalCoordinateReferenceSystemDatumName,
             HUCEightDigitCode, CountryCode, StateCode, CountyCode, OrganizationFormalName) %>%
      # replace lat/lon numeric flags with NAs
      mutate(LatitudeMeasure = ifelse(LatitudeMeasure < 10, NA, LatitudeMeasure),
             LongitudeMeasure = ifelse(LongitudeMeasure > -10, NA, LongitudeMeasure)),
    by='MonitoringLocationIdentifier')
  
  # write the data file and the indicator file
  out_file <- as_data_file(ind_file)
  saveRDS(wqp_info, out_file)
  sc_indicate(ind_file, data_file=out_file)
  
  invisible()
}

# produce a data frame of intended files and their contents
plan_wqp_files <- function() {
  
}

get_wqp_data <- function(out_file, state, wqp_state_codes, wqp_states_yml, wqp_codes_yml, wq_dates_yml) {
  wqp_states <- yaml::yaml.load_file(wqp_states_yml)
  wqp_codes <- yaml::yaml.load_file(wqp_codes_yml)
  wq_dates <- yaml::yaml.load_file(wq_dates_yml)
  state_code <- filter(wqp_state_codes, name==state) %>% pull(value)
  
  wqp_qdat_time <- system.time({
    wqp_qdat <- as_data_frame(readWQPdata(
      #StateCode=state_code,
      CountyCode="US:44:001",
      SiteType=wqp_codes$SiteType,
      CharacteristicName=unname(unlist(wqp_codes$CharacteristicName)),
      SampleMedia=wqp_codes$SampleMedia,
      StartDateLo=wq_dates$StartDate,
      StartDateHi=wq_dates$EndDate,
      querySummary=TRUE
    ))
  }) 
  wqp_dat_time <- system.time({
    wqp_dat <- readWQPdata(
      #StateCode=state_code,
      CountyCode="US:44:001",
      SiteType=wqp_codes$SiteType,
      CharacteristicName=unname(unlist(wqp_codes$CharacteristicName)),
      SampleMedia=wqp_codes$SampleMedia,
      StartDateLo=wq_dates$StartDate,
      StartDateHi=wq_dates$EndDate
    )
  }) 

  message("WQP pull for ", wqp_states, " took ", wqp_dat_time['elapsed'], " seconds")
    
  # this code is a placeholder to show how remake/scmake works
  feather::write_feather(as_data_frame(wq_dates), path=out_file)
}

# this code only makes sense if you ran the above code chunks manually, outside
# of their respective functions, and if you run the code within this function
# line-by-line.
compare_wqp_calls <- function() {
  dfs <- list(wqp_wdat, wqp_wsamp, wqp_wsite, wqp_wmet, wqp_qdat, wqp_dat)
  data_frame(
    query=c('whatWQPdata','whatWQPsamples','whatWQPsites','whatWQPmetrics','readWQPdata_querySummary','readWQPdata'),
    times=sapply(list(wqp_wdat_time, wqp_wsamp_time, wqp_wsite_time, wqp_wmet_time, wqp_qdat_time, wqp_dat_time), `[[`, 'elapsed'),
    nrow=sapply(dfs, nrow)
  )
  d1
}

get_wqp_state_codes <- function() {
  states_xml <- xml2::read_xml('https://www.waterqualitydata.us/Codes/statecode?countrycode=US')
  states_list <- xml2::as_list(states_xml)
  states_df <- bind_rows(lapply(states_list[names(states_list)=='Code'], function(code) {
    data_frame(
      value = attr(code, 'value'),
      name = attr(code, 'desc'),
      providers = attr(code, 'providers'))
  }))
  return(states_df)
}
