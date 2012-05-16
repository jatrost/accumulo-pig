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
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
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
public class AccumuloStorage extends AbstractAccumuloStorage
{
    private static final Log LOG = LogFactory.getLog(AccumuloStorage.class);

    public AccumuloStorage(){}

	@Override
	protected Tuple getTuple(Key key, Value value) throws IOException {
		// and wrap it in a tuple
        Tuple tuple = TupleFactory.getInstance().newTuple(6);
        tuple.set(0, new DataByteArray(key.getRow().getBytes()));
        tuple.set(1, new DataByteArray(key.getColumnFamily().getBytes()));
        tuple.set(2, new DataByteArray(key.getColumnQualifier().getBytes()));
        tuple.set(3, new DataByteArray(key.getColumnVisibility().getBytes()));
        tuple.set(4, new Long(key.getTimestamp()));
        tuple.set(5, new DataByteArray(value.get()));
        return tuple;
	}
	
	@Override
	public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
		Mutation mut = new Mutation(Utils.objToText(tuple.get(0)));
        Text cf = Utils.objToText(tuple.get(1));
    	Text cq = Utils.objToText(tuple.get(2));
    	
        if(tuple.size() > 4)
        {
        	Text cv = Utils.objToText(tuple.get(3));
        	Value val = new Value(Utils.objToBytes(tuple.get(4)));
        	if(cv.getLength() == 0)
        	{
        		mut.put(cf, cq, val);
        	}
        	else
        	{
        		mut.put(cf, cq, new ColumnVisibility(cv), val);
        	}
        }
        else
        {
        	Value val = new Value(Utils.objToBytes(tuple.get(3)));
        	mut.put(cf, cq, val);
        }
        
        return Collections.singleton(mut);
	}
}
