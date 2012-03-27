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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
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
public class AccumuloWholeRowStorage extends LoadFunc implements StoreFuncInterface
{
    private static final Log LOG = LogFactory.getLog(AccumuloWholeRowStorage.class);

    private Configuration conf;
    private RecordReader<Key, Value> reader;
    private RecordWriter<Text, Mutation> writer;
    
    String inst;
    String zookeepers;
    String user;
    String password;
    String table;
    Text tableName;
    String auths;
    Authorizations authorizations;
    List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text,Text>>();
    
    String start = null;
    String end = null;

    public AccumuloWholeRowStorage(){}

	@Override
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;
            
            Key key = (Key)reader.getCurrentKey();
            Value value = (Value)reader.getCurrentValue();
            assert key != null && value != null;
            
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
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
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
	
    @Override
    public InputFormat getInputFormat()
    {
        return new AccumuloInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
    }

    private void setLocationFromUri(String location) throws IOException
    {
        // ex: accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&columns=col1:cq1,col2:cq2&start=abc&end=z
        String names[];
        String columns = "";
        try
        {
            if (!location.startsWith("accumulo://"))
                throw new Exception("Bad scheme.");
            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1)
            {
                for (String param : urlParts[1].split("&"))
                {
                    String[] pair = param.split("=");
                    if (pair[0].equals("instance"))
                        inst = pair[1];
                    else if (pair[0].equals("user"))
                        user = pair[1];
                    else if (pair[0].equals("password"))
                        password = pair[1];
                    else if (pair[0].equals("zookeepers"))
                    	zookeepers = pair[1];
                    else if (pair[0].equals("auths"))
                    	auths = pair[1];
                    else if (pair[0].equals("columns"))
                    	columns = pair[1];
                    else if (pair[0].equals("start"))
                    	start = pair[1];
                    else if (pair[0].equals("end"))
                    	end = pair[1];
                }
            }
            String[] parts = urlParts[0].split("/+");
            table = parts[1];
            tableName = new Text(table);
            
            if(auths == null || auths.equals(""))
            {
            	authorizations = new Authorizations();
            }
            else
            {
            	authorizations = new Authorizations(auths.split(","));
            }
            
            if(!columns.equals("")){
            	for(String cfCq : columns.split(","))
            	{
            		if(cfCq.contains("|"))
            		{
            			String[] c = cfCq.split("\\|");
            			columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text(c[0]), new Text(c[1])));
            		}
            		else
            		{
            			columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text(cfCq), null));
            		}
            	}
            }
            	
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'accumulo://<table>[?instance=<instanceName>&user=<user>&password=<password>&zookeepers=<zookeepers>&auths=<authorizations>&[start=startRow,end=endRow,columns=[cf1:cq1,cf2:cq2,...]]]': " + e.getMessage());
        }
    }

    @Override
    public void setLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);
        
        if(!conf.getBoolean(AccumuloInputFormat.class.getSimpleName()+".configured", false))
        {
        	AccumuloInputFormat.setInputInfo(conf, user, password.getBytes(), table, authorizations);
            AccumuloInputFormat.setZooKeeperInstance(conf, inst, zookeepers);
            if(columnFamilyColumnQualifierPairs.size() > 0)
            {
            	LOG.info("columns: "+columnFamilyColumnQualifierPairs);
            	AccumuloInputFormat.fetchColumns(conf, columnFamilyColumnQualifierPairs);
            }
            
            AccumuloInputFormat.setRanges(conf, Collections.singleton(new Range(start, end)));
            AccumuloInputFormat.addIterator(conf, new IteratorSetting(10, WholeRowIterator.class));
        }
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException
    {
        return location;
    }

    @Override
    public void setUDFContextSignature(String signature)
    {
        
    }

    /* StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
        
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
    }
    
    public void setStoreLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);
        
        if(!conf.getBoolean(AccumuloOutputFormat.class.getSimpleName()+".configured", false))
        {
        	AccumuloOutputFormat.setOutputInfo(conf, user, password.getBytes(), true, table);
            AccumuloOutputFormat.setZooKeeperInstance(conf, inst, zookeepers);
            AccumuloOutputFormat.setMaxLatency(conf, 10*1000);
            AccumuloOutputFormat.setMaxMutationBufferSize(conf, 10*1000*1000);
            AccumuloOutputFormat.setMaxWriteThreads(conf, 10);
        }
    }

    public OutputFormat getOutputFormat()
    {
        return new AccumuloOutputFormat();
    }

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    public void prepareToWrite(RecordWriter writer)
    {
        this.writer = writer;
    }

    public void putNext(Tuple tuple) throws ExecException, IOException
    {
        Mutation mut = new Mutation(objToText(tuple.get(0)));
        DefaultDataBag columns = (DefaultDataBag)tuple.get(1);
        for(Tuple column : columns)
        {
        	Text cf = objToText(column.get(0));
        	Text cq = objToText(column.get(1));
        	Text cv = objToText(column.get(2));
        	Long ts = (Long)column.get(3);
        	Value val = new Value(objToBytes(column.get(4)));
        	
        	mut.put(cf, cq, new ColumnVisibility(cv), ts, val);
        }
        
        try {
			writer.write(tableName, mut);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
    }
    
    private static Text objToText(Object o)
    {
    	return new Text(objToBytes(o));
    }
        
    private static byte[] objToBytes(Object o)
    {
    	if (o instanceof String) {
			String str = (String) o;
			return str.getBytes();
		}
    	else if (o instanceof Long) {
			Long l = (Long) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Integer) {
    		Integer l = (Integer) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Boolean) {
    		Boolean l = (Boolean) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Float) {
    		Float l = (Float) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Double) {
    		Double l = (Double) o;
			return l.toString().getBytes();
		}
    	
    	// TODO: handle DataBag, Map<Object, Object>, and Tuple
    	
    	return ((DataByteArray)o).get();
    }

    public void cleanupOnFailure(String failure, Job job){}
}
