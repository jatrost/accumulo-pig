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

import org.apache.hadoop.io.Text;
import org.apache.pig.data.DataByteArray;

public class Utils {
	public static Text objToText(Object o)
    {
    	return new Text(objToBytes(o));
    }
    
    public static byte[] objToBytes(Object o)
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
}
