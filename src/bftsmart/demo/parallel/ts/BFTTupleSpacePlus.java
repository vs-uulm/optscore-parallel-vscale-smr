/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.demo.parallel.ts;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author alchieri
 */
public class BFTTupleSpacePlus extends BFTTupleSpace{
    
   
   // private int factor = 1;

    public BFTTupleSpacePlus(int id, boolean parallelExecution) {
        super(id, parallelExecution);
    }
    
    /*public void setGroupFactor(int f){
        this.factor = f;
    }*/
     
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
                //if(factor == 1){
                    rep = proxy.invokeParallel(out.toByteArray(), (tuple.getFields().length-1));
               // }else{
                  //  rep = proxy.invokeParallel(out.toByteArray(), factor*tuple.getFields().length);
                //}
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
                //if(factor == 1){
                    rep = proxy.invokeParallel(out.toByteArray(), (template.getFields().length-1));
               //}else{
               //     rep = proxy.invokeParallel(out.toByteArray(), factor*template.getFields().length);
               // }
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
            Logger.getLogger(BFTTupleSpacePlus.class.getName()).log(Level.SEVERE, null, ex);
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
               //if(factor == 1){
                    rep = proxy.invokeParallel(out.toByteArray(), (template.getFields().length-1));
               // }else{
               //     rep = proxy.invokeParallel(out.toByteArray(), factor*template.getFields().length);
              //  }
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
            Logger.getLogger(BFTTupleSpacePlus.class.getName()).log(Level.SEVERE, null, ex);
             return null;
        }
    }
    
}
