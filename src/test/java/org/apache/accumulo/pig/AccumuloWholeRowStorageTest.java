/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class AccumuloWholeRowStorageTest {
	
	@Test
	public void testConfiguration() throws IOException
	{
		AbstractAccumuloStorageTest test = new AbstractAccumuloStorageTest();
		
		AccumuloWholeRowStorage s = new AccumuloWholeRowStorage();
		
		Job actual = new Job();
		s.setLocation(test.getDefaultLoadLocation(), actual);
		Configuration actualConf = actual.getConfiguration();
		
		Job expected =  test.getDefaultExpectedLoadJob();
		Configuration expectedConf = expected.getConfiguration();
		AccumuloInputFormat.addIterator(expectedConf, new IteratorSetting(10, WholeRowIterator.class));
		
		TestUtils.assertConfigurationsEqual(expectedConf, actualConf);
	}
	
	public static Tuple generateTuple(String cf, String cq, String cv, Long ts, String val) throws ExecException
	{
		Tuple tuple = TupleFactory.getInstance().newTuple(5);
		tuple.set(0, new DataByteArray(cf.getBytes()));
		tuple.set(1, new DataByteArray(cq.getBytes()));
		tuple.set(2, new DataByteArray(cv.getBytes()));
		tuple.set(3, ts);
		tuple.set(4, new DataByteArray(val.getBytes()));
		return tuple;
	}
	
	@Test
	public void testGetMutations() throws Exception
	{
		Tuple tuple = TupleFactory.getInstance().newTuple(2);
		tuple.set(0, "row1");
		
		DefaultDataBag bag = new DefaultDataBag();
		bag.add(generateTuple("cf1", "cq1", "cv1", 1L, "val1"));
		bag.add(generateTuple("cf2", "cq2", "cv2", 2L, "val2"));
		bag.add(generateTuple("cf3", "cq3", "cv3", 3L, "val3"));
		tuple.set(1, bag);
		
		AccumuloWholeRowStorage s = new AccumuloWholeRowStorage();
		Collection<Mutation> muts = s.getMutations(tuple);
		
		assertNotNull(muts);
		assertEquals(1, muts.size());
		Mutation mut = muts.iterator().next();
		
		List<ColumnUpdate> updates = mut.getUpdates();
		assertEquals(3, updates.size());
		
		assertTrue(Arrays.equals(((String)tuple.get(0)).getBytes(), mut.getRow()));
		
		Iterator<Tuple> iter = bag.iterator();
		for(ColumnUpdate update : updates)
		{
			Tuple colTuple = iter.next();
			
			assertTrue(Arrays.equals(((DataByteArray)colTuple.get(0)).get(), update.getColumnFamily()));
			assertTrue(Arrays.equals(((DataByteArray)colTuple.get(1)).get(), update.getColumnQualifier()));
			assertTrue(Arrays.equals(((DataByteArray)colTuple.get(2)).get(), update.getColumnVisibility()));
			assertEquals(((Long)colTuple.get(3)).longValue(), update.getTimestamp());
			assertTrue(Arrays.equals(((DataByteArray)colTuple.get(4)).get(), update.getValue()));
		}
	}
	
	@Test
	public void testGetTuple() throws Exception
	{
		AccumuloWholeRowStorage s = new AccumuloWholeRowStorage();
		
		Key key = new Key("row");
		
		List<Key> keys = new ArrayList<Key>(3);
		keys.add(new Key("row", "cf1", "cf1", "cv1", 1L));
		keys.add(new Key("row", "cf2", "cf2", "cv2", 2L));
		keys.add(new Key("row", "cf3", "cf3", "cv3", 3L));
		
		List<Value> values = new ArrayList<Value>(3);
		values.add(new Value("1".getBytes()));
		values.add(new Value("2".getBytes()));
		values.add(new Value("3".getBytes()));
		
		Value value = WholeRowIterator.encodeRow(keys, values);
		
		List<Tuple> columns = new LinkedList<Tuple>();
		for(int i = 0; i < keys.size(); ++i)
		{
			columns.add(columnToTuple(
							keys.get(i).getColumnFamily().toString(), 
							keys.get(i).getColumnQualifier().toString(), 
							keys.get(i).getColumnVisibility().toString(), 
							keys.get(i).getTimestamp(), 
							new String(values.get(i).get())));
		}
		
		Tuple tuple = TupleFactory.getInstance().newTuple(2);
        tuple.set(0, new DataByteArray(key.getRow().getBytes()));
        tuple.set(1, new DefaultDataBag(columns));
        
		TestUtils.assertWholeRowKeyValueEqualsTuple(key, value, tuple);
	}
	
	private Tuple columnToTuple(String colfam, String colqual, String colvis, long ts, String val) throws IOException
    {
        Tuple tuple = TupleFactory.getInstance().newTuple(5);
        tuple.set(0, new DataByteArray(colfam.getBytes()));
        tuple.set(1, new DataByteArray(colqual.getBytes()));
        tuple.set(2, new DataByteArray(colvis.getBytes()));
        tuple.set(3, new Long(ts));
        tuple.set(4, new DataByteArray(val.getBytes()));
        return tuple;
    }
	
	
}
