#We need to split the data into two datasets. One with date and time and one with just date. 
#This takes kind of a long time so we will use foreach to run the analyses in parallel. 


#single and multiple obs harmonization that will live inside the next function. 
pluribus <- function(dt,x,...){
  #seperate out the data that has only 1 obs at unique site, datetime, and parameter combination. 
  #This should be most data and will simply be kept. 
  duplicated.index <- duplicated(dt %>% 
                                   select_('SiteID','harmonized_parameter',...))
  
  
  dt.not.unique <- dt %>%
    filter(duplicated.index) 
  

  #Okay lots of those have the same exact information. 
  #We can get rid of those kinds of duplicates with a simple duplicated call
  distinct.index <- duplicated(dt.not.unique %>% 
                                 select_('SiteID','harmonized_value',
                                         'harmonized_parameter',...))
  
  
  
  dt.distinct.okay <- dt.not.unique %>%
    filter(distinct.index) %>%
    #Add in the data that had no duplicates in the first place
    rbind(dt %>% filter(!duplicated.index))



  #What should we do with multiple data for the same site, time, and parameter? 
  dt.multiples <- dt.not.unique %>%
    filter(!distinct.index) %>%
    #The dots are so that the user can switch between datetime or date
    group_by_('SiteID','harmonized_parameter',...) %>%
    #Get the median and cv
    mutate(count=n()) %>%
    ungroup() %>%
    filter(count < x) %>%
    group_by_('SiteID','harmonized_parameter',...) %>%
    mutate(median=median(harmonized_value),
           cv=sd(harmonized_value)/mean(harmonized_value)) %>%
    ungroup() %>%
    #Remove sites with cvs greater than 0.1
    filter(cv < 0.1) %>%
    #Keep only a single observation with unique combinations
    distinct_('SiteID','harmonized_parameter',...,.keep_all=T) %>%
    mutate(harmonized_value = median) %>%
    dplyr::select(-median,-cv,-count) 
  
  return(rbind(dt.distinct.okay,dt.multiples) %>%
           #Last bit of paranoia to catch any NAs or doubles that sneak through
           distinct_('SiteID','harmonized_parameter',...,.keep_all=T))
}


#Function to split out data with date_time or date and time. 

date.time.splitter<- function(df) {
  #Add an index and remove nas
  dt <- df %>% 
    #Filter out data that has no date_time OR time data
    filter(!is.na(date_time))
  
  #Date only data
  d <- df %>%
    filter(!index %in% dt$index) 
  
  dt.final <- pluribus(dt,5,'date_time')
  d.final <- pluribus(d,20,'date')
  
  
  #Put these two datasets into a list
  out <- rbind(dt.final,d.final) %>%
    dplyr::select(-index)
  
  
  return(out)
}

