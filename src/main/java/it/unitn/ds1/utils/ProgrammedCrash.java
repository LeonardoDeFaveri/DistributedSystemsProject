package it.unitn.ds1.utils;

import java.util.HashMap;

/**
 * This class allows to program the happening of a crash, boundint it 
 */
public class ProgrammedCrash {
    private final HashMap<KeyEvents, Integer> targets;
    private final HashMap<KeyEvents, Boolean> befores;

    public ProgrammedCrash() {
        this.targets = new HashMap<>();
        this.befores = new HashMap<>();
    }

    /**
     * Programs a new crash for event.
     * @param event key event which triggers the crash
     * @param before should the crash happen before of after the event is served?
     * @param target how many times should the event occur before triggering a crash?
     */
    public void program(KeyEvents event, boolean before, int target) {
        this.targets.put(event, target);
        this.befores.put(event, before);
    }

    /**
     * Register the happening of an event.
     */
    public void register(KeyEvents event) {
        int target = this.targets.getOrDefault(event, 0);
        target--;
                
        if (target >= 0) {
            this.targets.put(event, target);
        }
    }

    /**
     * The crash should happen before or after the event is served?
     */
    public boolean isBefore(KeyEvents event) {
        return this.befores.getOrDefault(event, true);
    }

    /**
     * Has the event happened enough times to trigger a crash?
     */
    public boolean isTargetReached(KeyEvents event) {
        return this.targets.getOrDefault(event, 1) == 0;
    }
}
