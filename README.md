# watersat
Monitoring water quality from space!

# scipiper
This is an extremely new work in progress, intended to support projects for our team and a limited number of collaborators. But it would be great to try it here if everybody is game.

scipiper offers 3 features that will likely be useful to us:

1. Support for a shared cache: our code can all live on GitHub while our data can all live on Google Drive and Earth Engine. Each big data file only needs to get acquired or processed once; the rest of us only need to pull that file locally if we're running a process that requires that exact file.

2. Integration of a shared cache with a file/object dependency manager called `remake`. With this integration in place, we should all be able to contribute to keeping the shared cache consistent with the code on GitHub. We won't need to question whether we remembered to run that edited version of the script or whether the data on GD is still from last week; we'll know.

3. An expansion of `remake` from jobs into tasks, where many tasks might be part of a single job, and our project is a collection of jobs. This idea should eventually be useful for spreading tasks across a cluster, but even at the current development phase this idea will allow us to split the WQP pull (one job) into a separate task for each state, and to attempt all those tasks with fault tolerance and retries as needed.
