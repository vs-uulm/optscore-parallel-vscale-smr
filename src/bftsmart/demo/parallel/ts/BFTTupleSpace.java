/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.demo.parallel.ts;



import bftsmart.tom.ParallelServiceProxy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import bftsmart.parallelism.ParallelMapping;

/**
 *
 * @author alchieri
 */
public class BFTTupleSpace {
    
    static final int OUT = 1;
    static final int INP = 2;
    static final int RDP = 3;

    static final String WILDCARD = "*";
    
    protected ParallelServiceProxy proxy = null;
    protected ByteArrayOutputStream out = null;

    protected boolean parallel = false;

    public BFTTupleSpace(int id, boolean parallelExecution) {
        proxy = new ParallelServiceProxy(id);
        this.parallel = parallelExecution;
    }

     
    public void out(Tuple tuple){
     try {
            out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(OUT);
            ObjectOutputStream out1 = new ObjectOutputStream(out);
            out1.writeObject(tuple);
            out1.close();
            byte[] rep = null;
            if (parallel) {
                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONFLICT_ALL);
            } else {
                rep = proxy.invokeOrdered(out.toByteArray());
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(rep);
            ObjectInputStream in = new ObjectInputStream(bis);
            boolean ret = in.readBoolean();
            in.close();
            //return ret;

        } catch (IOException ex) {
            ex.printStackTrace();
            //return false;
        }
    }
    
    public Tuple rdp(Tuple template){
         try {
            out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(RDP);
            ObjectOutputStream out1 = new ObjectOutputStream(out);
            out1.writeObject(template);
            out1.close();
            byte[] rep = null;
            if (parallel) {
                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONFLICT_NONE);
            } else {
                rep = proxy.invokeOrdered(out.toByteArray());
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(rep);
            ObjectInputStream in = new ObjectInputStream(bis);
            Tuple ret = (Tuple)in.readObject();
            in.close();
            return ret;
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BFTTupleSpace.class.getName()).log(Level.SEVERE, null, ex);
             return null;
        }
    }
    
    public Tuple inp(Tuple template) {
       try {
            out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(INP);
            ObjectOutputStream out1 = new ObjectOutputStream(out);
            out1.writeObject(template);
            out1.close();
            byte[] rep = null;
            if (parallel) {
                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONFLICT_ALL);
            } else {
                rep = proxy.invokeOrdered(out.toByteArray());
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(rep);
            ObjectInputStream in = new ObjectInputStream(bis);
            Tuple ret = (Tuple)in.readObject();
            in.close();
            return ret;
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BFTTupleSpace.class.getName()).log(Level.SEVERE, null, ex);
             return null;
        }
    }
    
}
