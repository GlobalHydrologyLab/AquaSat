# we've been asked to please NOT use a cluster because a postdoc took down the
# entire WQP a couple of years ago by doing that, and 12 simultaneous requests
# would be way too many. Alternative ways we can speed up the pull:
# * subset spatially rather than temporally - loop over state/HUC rather than years
# * probably fastest to pull all variables at once
# * probably faster to request whole states at once rather than giving explicit site lists

# prepare a data.frame that maps state names to the FIPS codes used by WQP
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

# acquire a data frame of site x constituent information, with counts of
# observations per site-constituent combination and all the site metadata that
# looks useful
inventory_wqp <- function(ind_file, wqp_state_codes, wqp_states, wqp_codes) {
  # convert states list to FIPS list
  state_codes <- filter(wqp_state_codes, name %in% wqp_states) %>% pull(value)
  
  # identify available constituent sets
  constituents <- names(wqp_codes$characteristicName)
  
  # prepare the args to whatWQPdata. all arguments will be the same every time
  # except characteristicName, which we'll loop through to get separate counts
  # for each
  wqp_args <- list(
    statecode=state_codes,
    siteType=wqp_codes$siteType,
    characteristicName=NA, # to be updated each time through loop
    sampleMedia=wqp_codes$sampleMedia
    # we'd include dates, but they get ignored by the service behind whatWQPdata
  )
  
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
    samples %>%
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
  data_file <- as_data_file(ind_file)
  feather::write_feather(wqp_info, path=data_file)
  gd_put(ind_file, data_file) # sc_indicate(ind_file, data_file=data_file)
  
  invisible()
}

# wrap dplyr::filter for use within a remake command
filter_partitions <- function(partitions, pull_task) {
  dplyr::filter(partitions, PullTask==pull_task)
}

# package all the configuration information for a single partition into each row
# so that we know whether to re-pull a partition based solely on whether the
# contents of that row have changed. each row should therefore include the lists
# of sites and characteristic names.
partition_inventory <- function(inventory_ind, wqp_pull, wqp_state_codes, wqp_codes) {
  # read in the inventory, which includes all dates and is therefore a superset
  # of what we'll be pulling for a specific date range
  scipiper::sc_retrieve(inventory_ind)
  inventory <- feather::read_feather(scipiper::as_data_file(inventory_ind)) %>%
    select(Constituent, Site=MonitoringLocationIdentifier, StateCode, resultCount)
  
  # split up the work into manageably sized partitions (~max size specified in wqp_pull)
  partitions <- bind_rows(lapply(unique(inventory$Constituent), function(constituent) {
    bind_rows(lapply(unique(inventory$StateCode), function(statecode) {
      # an atomic group is a combination of parameters that can't be reasonably
      # split into multiple WQP pulls - in this case we're defining atomic
      # groups as distinct combinations of constituent (a group of
      # characteristicNames) and site ID. We could potentially split
      # constituents further, or pull multiple constituents at once, or pull an
      # entire state at once, but we're doing it this way
      atomic_groups <- inventory %>%
        filter(StateCode==statecode, Constituent == constituent) %>%
        group_by(Site) %>%
        summarize(NumObs=sum(resultCount)) %>%
        arrange(desc(NumObs))
      
      # split the full pull (combine atomic groups) into right-sized partitions
      # by an old but fairly effective paritioning heuristic: pick the number of
      # partitions desired, sort the atomic groups by descending size, and then
      # go down the list, each time adding the next atomic group to the
      # partition that's currently smallest
      num_partitions <- ceiling(sum(atomic_groups$NumObs) / wqp_pull$target_pull_size)
      partition_sizes <- rep(0, num_partitions)
      assignments <- rep(0, nrow(atomic_groups))
      for(i in seq_len(nrow(atomic_groups))) {
        size_i <- atomic_groups[[i,"NumObs"]]
        smallest_partition <- which.min(partition_sizes)
        assignments[i] <- smallest_partition
        partition_sizes[smallest_partition] <- partition_sizes[smallest_partition] + size_i
      }
      
      # create and return data.frame rows for binding in the two lapply loops
      state <- wqp_state_codes %>%
        filter(value==sprintf('US:%s', statecode)) %>%
        pull(name)
      atomic_groups %>%
        mutate(
          State=state,
          StateCode=statecode,
          Constituent=constituent,
          NumPulls=num_partitions,
          PullTask=sprintf('%s_%s_%03d', gsub(' ', '_', state), constituent, assignments))
    }))
  }))
  
  # prepare nested collections of other possible arguments to include in the readWQPdata calls
  parameter_codes <- bind_rows(lapply(unique(partitions$Constituent), function(constituent) {
    data_frame(Constituent=constituent, CharacteristicNames=wqp_codes$characteristicName[[constituent]]) %>%
      tidyr::nest(-Constituent, .key='Params')
  }))
  site_types <- data_frame(SiteType=wqp_codes$siteType)
  pull_tasks_df <- partitions %>%
    dplyr::group_by(PullTask, NumPulls, State, StateCode, Constituent) %>%
    tidyr::nest(Site, NumObs, .key="Sites") %>%
    dplyr::ungroup() %>%
    dplyr::left_join(parameter_codes, by='Constituent') %>%
    dplyr::mutate(SiteType=list(site_types))
  
  return(pull_tasks_df)
}


# prepare a plan for downloading (from WQP) and posting (to GD) one data file
# per state
plan_wqp_pull <- function(partitions, folders) {
  
  partition <- scipiper::create_task_step(
    step_name = 'partition',
    target_name = function(task_name, step_name, ...) {
      sprintf('partition_%s', task_name)
    },
    command = function(task_name, ...) {
      sprintf("filter_partitions(wqp_pull_partitions, I('%s'))", task_name)
    }
  )
  
  # steps: download, post
  download <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      scipiper::as_indicator(file.path(folders$tmp, sprintf('%s.feather', task_name)))
    },
    command = function(task_name, ...) {
      paste(
        "get_wqp_data(",
        "ind_file=target_name,",
        sprintf("partition=partition_%s,", task_name),
        "wq_dates=wq_dates)",
        sep="\n      ")
    }
  )
  
  post <- scipiper::create_task_step(
    step_name = 'post',
    target_name = function(task_name, step_name, ...) {
      scipiper::as_indicator(file.path(folders$out, sprintf('%s.feather', task_name)))
    },
    command = function(task_name, ...) {
      sprintf(
        paste(
          "gd_put(",
          "remote_ind=target_name,",
          "local_source='%s',",
          "mock_get=I('move'),",
          "on_exists=I('replace'))",
          sep="\n      "),
        scipiper::as_indicator(file.path(folders$tmp, sprintf('%s.feather', task_name))))
    }
  )
  
  retrieve <- scipiper::create_task_step(
    step_name = 'retrieve',
    target_name = function(task_name, step_name, ...) {
      file.path(folders$out, sprintf('%s.feather', task_name))
    },
    command = function(task_name, target_name, ...) {
      sprintf(
        paste(
          "gd_get(",
          "ind_file='%s')",
          sep="\n      "),
        scipiper::as_indicator(target_name))
    }
  )
  
  task_plan <- scipiper::create_task_plan(
    task_names=sort(partitions$PullTask),
    task_steps=list(partition, download, post, retrieve),
    final_steps='post',
    add_complete=FALSE, 
    indicator_dir=folders$log)
  
}

create_wqp_pull_makefile <- function(makefile, task_plan) {
  create_task_makefile(
    makefile=makefile, task_plan=task_plan,
    include='remake.yml',
    packages=c('dplyr', 'dataRetrieval', 'feather', 'scipiper'),
    file_extensions=c('ind','feather'))
}

get_wqp_data <- function(ind_file, partition, wq_dates) {
  
  # prepare the argument to pass to readWQPdata
  wqp_args <- list(
    characteristicName=partition$Params[[1]]$CharacteristicNames,
    startDateLo=wq_dates$StartDate,
    startDateHi=wq_dates$EndDate
  )
  # specify sites either by whole state and siteType (if we didn't split the
  # site into multiple site sets) or as a specific list of sites (otherwise)
  if(partition$NumPulls == 1) {
    wqp_args <- c(wqp_args, list(
      statecode=partition$StateCode,
      siteType=partition$SiteType[[1]]$SiteType
    ))
  } else {
    wqp_args <- c(wqp_args, list(
      siteid=partition$Sites[[1]]$Site
    ))
  }
  
  # do the data pull
  wqp_dat_time <- system.time({
    wqp_dat <- do.call(dataRetrieval::readWQPdata, wqp_args)
  }) 
  message(
    "WQP pull for ", partition$PullTask,
    " took ", wqp_dat_time['elapsed'], " seconds and",
    " returned ", nrow(wqp_dat), " rows")
  
  # make wqp_dat a tibble, converting either from data.frame (the usual case) or
  # NULL (if there are no results)
  wqp_dat <- as_data_frame(wqp_dat)
  
  # write the data and indicator file. do this even if there were 0 results
  # because remake expects this function to always create the target file
  data_file <- as_data_file(ind_file)
  feather::write_feather(as_data_frame(wqp_dat), path=data_file)
  sc_indicate(ind_file, data_file=data_file)
  
  invisible()
}

