#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Google Earth Engine Reflectance Pull Functions
Created on Mon Apr  9 14:24:13 2018
@author: simontopp
"""
import ee

# Add filler panchromatic band to landsat 5 images.
def addPan(img):
  wPan = img.addBands(ee.Image(-999).rename('B8'))
  return wPan

