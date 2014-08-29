package org.hf.mls.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by He Fan on 2014/7/14.
 */
public class OptionsHelper {

    Map<String, String> optionsPairs = new HashMap<String, String>();

    public void setOptionsPairs(Map<String, String> optionsPairs) {
        this.optionsPairs = optionsPairs;
    }

    public Map<String, String> getOptionsPairs() {
        return optionsPairs;
    }

    public OptionsHelper(String[] args,String separatorChar) {
        if (0 == args.length % 2) {
            Map<String, String> optionsMap = new HashMap<String, String>();
            for (int i = 0; i < args.length; i += 2) {
                if (!args[i].startsWith(separatorChar)) {
                    setOptionsPairs(null);
                    return;
                }
                optionsMap.put(args[i].substring(separatorChar.length()), args[i + 1]);
            }
            setOptionsPairs(optionsMap);
        } else {
            setOptionsPairs(null);
        }
    }
}
