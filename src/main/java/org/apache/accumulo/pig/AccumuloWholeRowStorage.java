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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo
 *
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis, timestamp, value). All fields except timestamp are DataByteArray, timestamp is a long.
 * 
 * Tuples can be written in 2 forms:
 *  (key, colfam, colqual, colvis, value)
 *    OR
 *  (key, colfam, colqual, value)
 * 
 */
public class AccumuloWholeRowStorage extends AbstractAccumuloStorage
{
    private static final Log LOG = LogFactory.getLog(AccumuloWholeRowStorage.class);

    public AccumuloWholeRowStorage(){}

	@Override
	protected Tuple getTuple(Key key, Value value) throws IOException {
		
		SortedMap<Key, Value> rowKVs =  WholeRowIterator.decodeRow(key, value);
        List<Tuple> columns = new ArrayList<Tuple>(rowKVs.size());
        for(Entry<Key, Value> e : rowKVs.entrySet())
        {
        	columns.add(columnToTuple(
        			e.getKey().getColumnFamily(), 
        			e.getKey().getColumnQualifier(), 
        			e.getKey().getColumnVisibility(), 
        			e.getKey().getTimestamp(), 
        			e.getValue())
        		);
        }
        
        // and wrap it in a tuple
        Tuple tuple = TupleFactory.getInstance().newTuple(2);
        tuple.set(0, new DataByteArray(key.getRow().getBytes()));
        tuple.set(1, new DefaultDataBag(columns));
        
        return tuple;
	}

	private Tuple columnToTuple(Text colfam, Text colqual, Text colvis, long ts, Value val) throws IOException
    {
        Tuple tuple = TupleFactory.getInstance().newTuple(5);
        tuple.set(0, new DataByteArray(colfam.getBytes()));
        tuple.set(1, new DataByteArray(colqual.getBytes()));
        tuple.set(2, new DataByteArray(colvis.getBytes()));
        tuple.set(3, new Long(ts));
        tuple.set(4, new DataByteArray(val.get()));
        return tuple;
    }
	
    protected void configureInputFormat(Configuration conf)
    {
    	AccumuloInputFormat.addIterator(conf, new IteratorSetting(10, WholeRowIterator.class));
    }
    
    @Override
    public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
    	
    	Mutation mut = new Mutation(Utils.objToText(tuple.get(0)));
        DefaultDataBag columns = (DefaultDataBag)tuple.get(1);
        for(Tuple column : columns)
        {
        	Text cf = Utils.objToText(column.get(0));
        	Text cq = Utils.objToText(column.get(1));
        	Text cv = Utils.objToText(column.get(2));
        	Long ts = (Long)column.get(3);
        	Value val = new Value(Utils.objToBytes(column.get(4)));
        	
        	mut.put(cf, cq, new ColumnVisibility(cv), ts, val);
        }
    	
    	return Collections.singleton(mut);
    }
}
