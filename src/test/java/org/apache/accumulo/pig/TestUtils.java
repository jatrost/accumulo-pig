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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;

public class TestUtils {
	public static void assertConfigurationsEqual(Configuration expectedConf, Configuration actualConf)
	{
		// Basically, for all the keys in expectedConf, make sure the values in both confs are equal
        Iterator<Entry<String, String>> expectedIter = expectedConf.iterator();
        while(expectedIter.hasNext())
        {
        	Entry<String, String> e = expectedIter.next();
        	assertEquals(actualConf.get(e.getKey()), expectedConf.get(e.getKey()));
        }
        
        // Basically, for all the keys in actualConf, make sure the values in both confs are equal
        Iterator<Entry<String, String>> actualIter = actualConf.iterator();
        while(actualIter.hasNext())
        {
        	Entry<String, String> e = actualIter.next();
        	assertEquals(actualConf.get(e.getKey()), expectedConf.get(e.getKey()));
        }
	}
	
	public static void assertKeyValueEqualsTuple(Key key, Value value, Tuple tuple) throws ExecException
	{
		assertTrue(Arrays.equals(key.getRow().getBytes(), ((DataByteArray)tuple.get(0)).get()));
		assertTrue(Arrays.equals(key.getColumnFamily().getBytes(), ((DataByteArray)tuple.get(1)).get()));
		assertTrue(Arrays.equals(key.getColumnQualifier().getBytes(), ((DataByteArray)tuple.get(2)).get()));
		assertTrue(Arrays.equals(key.getColumnVisibility().getBytes(), ((DataByteArray)tuple.get(3)).get()));
		assertEquals(key.getTimestamp(), ((Long)tuple.get(4)).longValue());
		assertTrue(Arrays.equals(value.get(), ((DataByteArray)tuple.get(5)).get()));
	}
	
	public static void assertWholeRowKeyValueEqualsTuple(Key key, Value value, Tuple mainTuple) throws IOException
	{
		assertTrue(Arrays.equals(key.getRow().getBytes(), ((DataByteArray)mainTuple.get(0)).get()));
		
		DefaultDataBag bag = (DefaultDataBag)mainTuple.get(1);
		Iterator<Tuple> iter = bag.iterator();		
		
		for(Entry<Key, Value> e : WholeRowIterator.decodeRow(key, value).entrySet())
		{
			Tuple tuple = iter.next();
			
			assertTrue(Arrays.equals(e.getKey().getColumnFamily().getBytes(), ((DataByteArray)tuple.get(0)).get()));
			assertTrue(Arrays.equals(e.getKey().getColumnQualifier().getBytes(), ((DataByteArray)tuple.get(1)).get()));
			assertTrue(Arrays.equals(e.getKey().getColumnVisibility().getBytes(), ((DataByteArray)tuple.get(2)).get()));
			assertEquals(e.getKey().getTimestamp(), ((Long)tuple.get(3)).longValue());
			assertTrue(Arrays.equals(e.getValue().get(), ((DataByteArray)tuple.get(4)).get()));
		}
	}
}
