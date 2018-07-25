# watersat
Monitoring water quality from space!

##  PYTHON INFORMATION GOES HERE ##
* list the python packages that need to be installed, etc.

## using `scipiper`

The `scipiper` package is an extremely young work in progress, intended to support projects for our USGS data science team and a limited number of collaborators. (Specifically, the code is public but we're not planning to provide any support for projects we're not directly involved in.) APA would really like to try it here if everybody is game. Install scipiper from GitHub:

```r
devtools::install_github('USGS-R/scipiper', update_dependencies = TRUE)
```

`scipiper` offers 3 features that will likely be useful to us:

1. Support for a shared cache: our code can all live on GitHub while our data can all live on Google Drive and Earth Engine. Each big data file only needs to get acquired or processed once; the rest of us only need to pull that file locally if we're running a process that requires that exact file.

2. Integration of a shared cache with a file/object dependency manager called `remake` (https://github.com/richfitz/remake). With this integration in place, we should all be able to contribute to keeping the shared cache consistent with the code on GitHub. We won't need to question whether we remembered to run that edited version of the script or whether the data on GD is still from last week; we'll know.

3. An expansion of `remake` from jobs into tasks, where many tasks might be part of a single job, and our project is a collection of jobs. This idea should eventually be useful for spreading tasks across a cluster, but even at the current development phase this idea will allow us to split the WQP pull (one job) into a separate task for each state, and to attempt all those tasks with fault tolerance and retries as needed.

The main way we'd use `scipiper` would be in building the project, in conjunction with development of the remake.yml file to declare the relationships among our raw, intermediate, and end-product data files. To build a specific file, along with any upstream dependencies that might be out of date, call `scmake()` on that file. For example:

```r
library(scipiper)
scmake('1_wqdata/out/wqp_wisconsin.feather')
```

Or to build any out-of-date targets in the entire project, simply:
```r
scmake()
```
