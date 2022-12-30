/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package bftsmart.demo.parallel.ts;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * @author alchieri
 */
public class Tuple implements Externalizable {
    //public class DepTuple implements Serializable {
    
    private Object[] fields;
    
    
    /** private constructor, called by 'create' methods */
    public Tuple() {
        
        this.fields = null;
        
    }
    
    
    
    /** private constructor, called by 'create' methods */
    private Tuple(Object[] fields) {
        this.fields = fields;
    }
    
  
    public void readExternal(ObjectInput in)   throws IOException,
            ClassNotFoundException{
          
        int tam = in.readInt();
        if(tam < 0){
            fields = null;
        }else{
            fields = new Object[tam];
            for(int i = 0; i < tam; i++){
                fields[i] = in.readObject();
            }
        }
      }
    
    public void writeExternal(ObjectOutput out) throws IOException{
        
        
        if(fields != null){
            //out.writeInt(1);
            int tam = fields.length;
            out.writeInt(tam);
            for(int i = 0; i < tam; i++){
                out.writeObject(fields[i]);
            }
        }else{
            out.writeInt(-1);
        }
    }
    
    //create methods for several kinds of tuples
    
    public static final Tuple createTuple(Object... fields) {
        return new Tuple(fields);
    }
        
    //acessor methods
    
    public final Object[] getFields() {
        return fields;
    }
    
    
    public boolean equals(Object obj) {
        if(!(obj instanceof Tuple)) {
            return false;
        }
        
        Tuple tuple = (Tuple)obj;
        
        
        return equalFields(tuple);
    }
    
    
    public boolean equalFields(Tuple tuple) {
        Object[] fields = tuple.getFields();
        
        if(fields == this.fields){
            return true;
        }
        
        if(fields == null || this.fields == null){
            return false;
        }
        
        
        int m = this.fields.length;
        
        if(fields.length != m) {
            return false;
        }
        
        for(int i=0; i<m; i++) {
            if(!fields[i].equals(this.fields[i])) {
                return false;
            }
        }
        
        return true;
    }
    
    public String toString() {
        StringBuffer buff = new StringBuffer(1024);
        buff.append("[");
        if(fields != null && fields.length > 0){
            buff.append(fields[0]);
            for(int i=1; i<fields.length; i++) {
                buff.append(",").append(fields[i]);
            }
        }
        buff.append("]");
        
        
        return buff.toString();
    }
    
}
