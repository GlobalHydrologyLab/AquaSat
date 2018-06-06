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
  date = ee.Date(i.get('date'))
    
  #Create a buffer around the sample site. Size is determined above.
  sdist = i.geometry().buffer(dist)
    
  #Filter the landsat scenes associated with the path/row to the sample date
  #and clip it to the site buffer
  lsSample = ee.Image(lsover.filterDate(date,date.advance(1,'day')).first()).clip(sdist)

  #Create a mask that removes pixels identifed as cloud or cloud 
  #shadow with the pixel qa band
  
  #For each collection identify bits associates with each obstruction.
  if collection == 'SR':
    mission = ee.String(lsSample.get('SATELLITE')).split('_').get(1)
    bitAffected = {
    'Cloud': [5, 1],
    'CloudShadow': [3, 1],
    'CirrusConfidence': [8,2] #only present for L8, but bits aren't used in 5-7 
                              #so will just come up empty
    
    #'SnowIceConfidence': [9, 2]  Realized that sometimes super turbid waterbodies are
    #flagged as snow/ice, subsequently you can't filter by this unless you know your area
    #is unaffected by commission errors.
    }
  
  else:
    mission = ee.String(lsSample.get('SPACECRAFT_ID')).split('_').get(1)
    bitAffected = {
    'Cloud': [4, 1],
    'CloudShadow': [7, 2],
    'CirrusConfidence': [11,2]
    #'SnowIceConfidence': [9, 2]
    }
  

  ## Create layer to mask out roads that might not show up in Pekel and potentially 
  #corrupt pixel values  
  road = ee.FeatureCollection("TIGER/2016/Roads").filterBounds(sdist)\
  .geometry().buffer(30) 
  
  #Select qa band
  qa = lsSample.select('qa')
    
  #Create road, cloud, and shadow mask. 
  
  #Upack quality band to identify clouds, cloud shadows, and cirrus shadows.
  #For SR collections, clouds and cloud shadows will be either 0 or 1.  For
  #TOA collection, cloud shadow will be 1,2, or 3, associated with low, medium, or high
  #confidence respectively.  The following code only removes high confidence cloud shadows and cirrus
  #clouds, but this can be changed to accomadate specific research goals/areas.
  
  qaUnpack = UnpackAll(qa, bitAffected)
  
  if collection == 'SR':
    mask = qaUnpack.select('Cloud').eq(1)\
    .Or(qaUnpack.select('CloudShadow').eq(1))\
    .Or(qaUnpack.select('CirrusConfidence').eq(3))\
    .paint(road,1).Not()
  else:
    mask = qaUnpack.select('Cloud').eq(1)\
    .Or(qaUnpack.select('CloudShadow').eq(3))\
    .Or(qaUnpack.select('CirrusConfidence').eq(3))\
    .paint(road,1).Not()
 
  #Create water only mask
  wateronly = water.clip(sdist)
    
  #Update mask on imagery and add Pekel occurrence band for data export.
  lsSample = lsSample.addBands(pekel.select('occurrence'))\
  .updateMask(wateronly).updateMask(mask)
    
  #Collect median reflectance and occurance values
  lsout = lsSample.reduceRegion(ee.Reducer.median(), sdist, 30)
  
  #Collect reflectance and occurence st.dev.
  lsdev = lsSample.reduceRegion(ee.Reducer.stdDev(), sdist, 30)
    
  #Create dictionaries of median values and attach them to original site feature.
    
  output = i.set({'sat': mission})\
  .set({"blue": lsout.get('Blue')})\
  .set({"green": lsout.get('Green')})\
  .set({"red": lsout.get('Red')})\
  .set({"nir": lsout.get('Nir')})\
  .set({"swir1": lsout.get('Swir1')})\
  .set({"swir2": lsout.get('Swir2')})\
  .set({"qa": lsout.get('qa')})\
  .set({"blue_sd": lsdev.get('Blue')})\
  .set({"green_sd": lsdev.get('Green')})\
  .set({"red_sd": lsdev.get('Red')})\
  .set({"nir_sd": lsdev.get('Nir')})\
  .set({"swir1_sd": lsdev.get('Swir1')})\
  .set({"swir2_sd": lsdev.get('Swir2')})\
  .set({"qa_sd": lsdev.get('qa')})\
  .set({"pixelCount": lsSample.reduceRegion(ee.Reducer.count(), sdist, 30).get('Blue')})\
  .set({'path': lsSample.get('WRS_PATH')})\
  .set({'row': lsSample.get('WRS_ROW')})
  
  if collection == 'TOA':
    output = output.set({"pan": lsout.get('Pan')})
    
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
    
