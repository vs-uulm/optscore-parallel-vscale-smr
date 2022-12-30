package de.optscore.vscale;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EvalRequest {

    private final int[][] actions;

    private EvalRequest(int[][] actions) {
        this.actions = actions;
    }

    public int[][] getActions() {
        return actions;
    }

    public static class EvalRequestBuilder {

        private List<Integer> actions;
        private List<Integer> parameters;

        public EvalRequestBuilder() {
            this.actions = new ArrayList<>(6);
            this.parameters = new ArrayList<>(6);
        }

        public EvalRequestBuilder(int evalActionType, int parameter) {
            this.actions = new ArrayList<>(6);
            this.parameters = new ArrayList<>(6);
            this.actions.add(evalActionType);
            this.parameters.add(parameter);
        }

        public EvalRequestBuilder action(int evalActionType, int parameter) {
            this.actions.add(evalActionType);
            this.parameters.add(parameter);
            return this;
        }

        public EvalRequest build() {
            int[][] reqActions = new int[this.actions.size()][2];
            for(int i = 0; i < this.actions.size(); i++) {
                reqActions[i][0] = this.actions.get(i);
                reqActions[i][1] = this.parameters.get(i);
            }
            return new EvalRequest(reqActions);
        }
    }

    public static byte[] serializeEvalRequest(EvalRequest request) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        // sequentially write action-parameter array to byte-array (action|parameter|action|parameter|etc...)
        try {
            int[][] actions = request.getActions();
            for(int[] action : actions) {
                dos.writeInt(action[0]);
                dos.writeInt(action[1]);
            }
            return bos.toByteArray();
        } catch(IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}