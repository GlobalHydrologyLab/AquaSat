#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Google Earth Engine Reflectance Pull Functions
Created on Mon Apr  9 14:24:13 2018
@author: simontopp
"""

# Add filler panchromatic band to landsat 5 images.
def addPan(img):
  wPan = img.addBands(ee.Image(-999).rename('B8'))
  return wPan

###These are functions for unpacking the bit quality assessment band for TOA  
def Unpack(bitBand, startingBit, bitWidth):
  #unpacking bit bands
  #see: https://groups.google.com/forum/#!starred/google-earth-engine-developers/iSV4LwzIW7A
  return (ee.Image(bitBand)\
  .rightShift(startingBit)\
  .bitwiseAnd(ee.Number(2).pow(ee.Number(bitWidth)).subtract(ee.Number(1)).int()))
  
def UnpackAll(bitBand, bitInfo):
  unpackedImage = ee.Image.cat([Unpack(bitBand, bitInfo[key][0], bitInfo[key][1]).rename([key]) for key in bitInfo])
  return unpackedImage

####  This function maps across all the sites in a given Path/Row file and 
# extracts reflectance data for each in situ sampling date after creating a water mask.
def sitePull(i):

  #Pull the overpass date associated with the sample (+/- 1 day)
  date = ee.Date(i.get('Date'))
    
  #Create a buffer around the sample site. Size is determined above.
  sdist = i.geometry().buffer(dist)
    
  #Filter the landsat scenes associated with the path/row to the sample date
  #and clip it to the site buffer
  lsSample = ee.Image(lsover.filterDate(date,date.advance(1,'day')).first()).clip(sdist)

  #Create a mask that removes pixels identifed as cloud or cloud 
  #shadow with the pixel qa band
  if collection == 'SR':
    mission = ee.String(lsSample.get('SATELLITE')).split('_').get(1)
  else:
    mission = ee.String(lsSample.get('SPACECRAFT_ID')).split('_').get(1)
  
  bitAffected = {
    'Cloud': [4, 1],
    'CloudShadowConfidence': [7, 2],
    'SnowIceConfidence': [9, 2]
    }
  
  #Include cirrus confidence for landsat 8.  
  if mission == 8:
    bitAffected['CirrusConfidence'] = [11,2]
    
  
  ## Burn in roads that might not show up in Pekel and potentially 
  #corrupt pixel values  
  road = ee.FeatureCollection("TIGER/2016/Roads").filterBounds(sdist)\
  .geometry().buffer(30) 
  
  ##Cloud/Shadow masking info for SR collections. Consider changing this to match TOA
  # masking at some point.
  cloudShadowBitMask = ee.Number(2).pow(3).int()
  cloudsBitMask = ee.Number(2).pow(5).int()
  snowBitMask = ee.Number(2).pow(4).int()
  qa = lsSample.select('qa')
    
  #Create road, cloud, and shadow mask. TOA and SR require different 
  #functions both defined above
  if collection == "SR":
    mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0)\
    .And(qa.bitwiseAnd(cloudsBitMask).eq(0))\
    .And(qa.bitwiseAnd(snowBitMask).eq(0))\
    .paint(road,0)

  else:
    qaUnpack = UnpackAll(qa, bitAffected)
    if mission == 8:
      mask = qaUnpack.select('Cloud').eq(1)\
      .Or(qaUnpack.select('CloudShadowConfidence').eq(3))\
      .Or(qaUnpack.select('SnowIceConfidence').eq(3))\
      .Or(qaUnpack.select('CirrusConfidence').eq(3))\
      .paint(road,1).Not()
    else:
      mask = qaUnpack.select('Cloud').eq(1)\
        .Or(qaUnpack.select('CloudShadowConfidence').eq(3))\
        .Or(qaUnpack.select('SnowIceConfidence').eq(3))\
        .paint(road,1).Not()
 
  #Create water only mask
  wateronly = water.clip(sdist)
    
  #Update mask on imagery and add Pekel occurrence band for data export.
  lsSample = lsSample.addBands(pekel.select('occurrence'))\
  .updateMask(wateronly).updateMask(mask)
    
  #Collect mean reflectance and occurance values
  lsout = lsSample.reduceRegion(ee.Reducer.median(), sdist, 30)
    
  #Create dictionaries of median values and attach them to original site feature.
    
  output = i.set({'sat': mission})\
  .set({"Blue": lsout.get('Blue')})\
  .set({"Green": lsout.get('Green')})\
  .set({"Red": lsout.get('Red')})\
  .set({"Nir": lsout.get('Nir')})\
  .set({"Swir1": lsout.get('Swir1')})\
  .set({"Swir2": lsout.get('Swir2')})\
  .set({"qa": lsout.get('qa')})\
  .set({"pwater": lsout.get('occurrence')})\
  .set({"pixelCount": lsSample.reduceRegion(ee.Reducer.count(), sdist, 30).get('Blue')})\
  .set({'PATH': lsSample.get('WRS_PATH')})\
  .set({'ROW': lsSample.get('WRS_ROW')})
  
  if collection == 'TOA':
    output = output.set({"Pan": lsout.get('Pan')})
    
  return output

##Function for limiting the max number of tasks sent to
#earth engine at one time to avoid time out errors

def maximum_no_of_tasks(MaxNActive, waitingPeriod):
  ##maintain a maximum number of active tasks
  time.sleep(10)
  ## initialize submitting jobs
  ts = list(ee.batch.Task.list())

  NActive = 0
  for task in ts:
       if ('RUNNING' in str(task) or 'READY' in str(task)):
           NActive += 1
  ## wait if the number of current active tasks reach the maximum number
  ## defined in MaxNActive
  while (NActive >= MaxNActive):
      time.sleep(waitingPeriod) # if reach or over maximum no. of active tasks, wait for 2min and check again
      ts = list(ee.batch.Task.list())
      NActive = 0
      for task in ts:
        if ('RUNNING' in str(task) or 'READY' in str(task)):
          NActive += 1
  return()
    
