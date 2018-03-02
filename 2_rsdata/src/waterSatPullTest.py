import ee
ee.Initialize()
import pandas as p
import feather as f

pekel = ee.Image('JRC/GSW1_0/GlobalSurfaceWater')

file = '2_rsdata/tmp/uniqueInventory.feather'

inv = f.read_dataframe(file)

invOut = ee.FeatureCollection([ee.Feature(ee.Geometry.Point([inv['long'][i]\
                                                      , inv['lat'][i]]),\
                                          {'SiteID':inv['SiteID'][i]\
                                           }) for i in range(5)])



occ = pekel.select('occurrence')

def waterfunc(buf):
    invBuf = buf.buffer(200).geometry()
    pekclip = occ.clip(invBuf)
    pekMin = pekclip.reduceRegion(ee.Reducer.minMax(), invBuf, 30)
  
    out = buf.set({'max':pekMin.get('occurrence_max')})\
    .set({'min':pekMin.get('occurrence_min')})
 
    return out

outdata = invOut.map(waterfunc)

ee.batch.Export.table.toDrive(collection = outdata,
                                  description = 'LandsatInventory',
                                  folder='2_rsdata', fileFormat = 'csv').start()
