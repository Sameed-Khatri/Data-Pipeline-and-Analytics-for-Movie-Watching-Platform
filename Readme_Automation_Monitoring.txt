--> The ETLpipelineWithStagingArea_PYT folder contains runETL batch file that executes both
      ExtractTransform.py and TransformLoad.py respectively automating the pipeline through
      one master function.


--> The powerBi dashboards are connected to the archive and live datamarts (mysql databases/datamarts)
      so everytime the snapshot of spaghetti is taken i.e. the pipeline is run, the data in both the datamarts is
      updated and the dashboard refreshes the data to get the updated one displaying it via the charts and demographics.
