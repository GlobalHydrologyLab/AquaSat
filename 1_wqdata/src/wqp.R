# we've been asked to please NOT use a cluster because a postdoc took down the
# entire WQP a couple of years ago by doing that, and 12 simultaneous requests
# would be way too many. Alternative ways we can speed up the pull:
# * subset spatially rather than temporally - loop over state/HUC rather than years
# * probably fastest to pull all variables at once
# * probably faster to request whole states at once rather than giving explicit site lists

inventory_wqp <- function(wqp_codes_yml, wq_dates_yml) {
  wqp_codes <- yaml::yaml.load_file(wqp_codes_yml)
  wqp_dates <- yaml::yaml.load_file(wqp_dates_yml)
}

get_state_wqp <- function(state, wqp_codes_yml, wq_dates_yml) {
  wqp_codes <- yaml::yaml.load_file(wqp_codes_yml)
  wqp_dates <- yaml::yaml.load_file(wqp_dates_yml)

  secchi.dat <- readWQPdata(startDateLo=wqp_dates, startDateHi=startDateHi,characteristicNames=secchi.names)
  
}
