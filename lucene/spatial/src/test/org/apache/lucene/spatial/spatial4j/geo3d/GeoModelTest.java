package org.apache.lucene.spatial.spatial4j.geo3d;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test basic plane functionality.
 */
public class GeoModelTest {

  protected final static PlanetModel scaledModel = new PlanetModel(1.2,1.5);
  
  @Test
  public void testBasicCircle() {
    // The point of this test is just to make sure nothing blows up doing normal things with a quite non-spherical model
    // Make sure that the north pole is in the circle, and south pole isn't
    final GeoPoint northPole = new GeoPoint(scaledModel, Math.PI * 0.5, 0.0);
    final GeoPoint southPole = new GeoPoint(scaledModel, -Math.PI * 0.5, 0.0);
    final GeoPoint point1 = new GeoPoint(scaledModel, Math.PI * 0.25, 0.0);
    final GeoPoint point2 = new GeoPoint(scaledModel, Math.PI * 0.125, 0.0);
    
    GeoCircle circle = new GeoCircle(scaledModel, Math.PI * 0.5, 0.0, 0.01);
    assertTrue(circle.isWithin(northPole));
    assertFalse(circle.isWithin(southPole));
    assertFalse(circle.isWithin(point1));
    Bounds bounds = circle.getBounds(null);
    assertTrue(bounds.checkNoLongitudeBound());
    assertTrue(bounds.checkNoTopLatitudeBound());
    assertFalse(bounds.checkNoBottomLatitudeBound());
    assertEquals(Math.PI * 0.5 - 0.01, bounds.getMinLatitude(), 0.01);

    circle = new GeoCircle(scaledModel, Math.PI * 0.25, 0.0, 0.01);
    assertTrue(circle.isWithin(point1));
    assertFalse(circle.isWithin(northPole));
    assertFalse(circle.isWithin(southPole));
    bounds = circle.getBounds(null);
    assertFalse(bounds.checkNoTopLatitudeBound());
    assertFalse(bounds.checkNoLongitudeBound());
    assertFalse(bounds.checkNoBottomLatitudeBound());
    assertEquals(1.20, bounds.getMaxLatitude(), 0.01);
    assertEquals(Math.PI * 0.25 - 0.01, bounds.getMinLatitude(), 0.01);
    assertEquals(-0.36, bounds.getLeftLongitude(), 0.01);
    assertEquals(0.36, bounds.getRightLongitude(), 0.01);

    circle = new GeoCircle(scaledModel, Math.PI * 0.125, 0.0, 0.01);
    assertTrue(circle.isWithin(point2));
    assertFalse(circle.isWithin(northPole));
    assertFalse(circle.isWithin(southPole));
    bounds = circle.getBounds(null);
    assertFalse(bounds.checkNoLongitudeBound());
    assertFalse(bounds.checkNoTopLatitudeBound());
    assertFalse(bounds.checkNoBottomLatitudeBound());
    // Asymmetric, as expected
    assertEquals(Math.PI * 0.125 - 0.01, bounds.getMinLatitude(), 0.01);
    assertEquals(0.74, bounds.getMaxLatitude(), 0.01);
    assertEquals(-0.18, bounds.getLeftLongitude(), 0.01);
    assertEquals(0.18, bounds.getRightLongitude(), 0.01);

  }

  @Test
  public void testBasicRectangle() {
    final GeoBBox bbox = GeoBBoxFactory.makeGeoBBox(scaledModel, 1.0, 0.0, 0.0, 1.0);
    final GeoPoint insidePoint = new GeoPoint(scaledModel, 0.5, 0.5);
    assertTrue(bbox.isWithin(insidePoint));
    final GeoPoint topOutsidePoint = new GeoPoint(scaledModel, 1.01, 0.5);
    assertFalse(bbox.isWithin(topOutsidePoint));
    final GeoPoint bottomOutsidePoint = new GeoPoint(scaledModel, -0.01, 0.5);
    assertFalse(bbox.isWithin(bottomOutsidePoint));
    final GeoPoint leftOutsidePoint = new GeoPoint(scaledModel, 0.5, -0.01);
    assertFalse(bbox.isWithin(leftOutsidePoint));
    final GeoPoint rightOutsidePoint = new GeoPoint(scaledModel, 0.5, 1.01);
    assertFalse(bbox.isWithin(rightOutsidePoint));
    final Bounds bounds = bbox.getBounds(null);
    assertFalse(bounds.checkNoLongitudeBound());
    assertFalse(bounds.checkNoTopLatitudeBound());
    assertFalse(bounds.checkNoBottomLatitudeBound());
    assertEquals(1.0, bounds.getMaxLatitude(), 0.00001);
    assertEquals(0.0, bounds.getMinLatitude(), 0.00001);
    assertEquals(1.0, bounds.getRightLongitude(), 0.00001);
    assertEquals(0.0, bounds.getLeftLongitude(), 0.00001);
  }
  
}


