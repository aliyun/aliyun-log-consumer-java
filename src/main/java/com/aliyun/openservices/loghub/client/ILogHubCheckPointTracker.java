package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

/**
 * Provide an interface for the user to save a shard's check point to the
 * outside system. It provide two mode to save check point : to memory / to
 * persistent system
 * <p>
 * when the shard is taken by other worker, the ILogHubCheckPointTracker will
 * try to persistent the last save checkpoint in memory to out side system
 */
public interface ILogHubCheckPointTracker {
    /**
     * Save the check point into the outside system or just save into memory
     * according to the parameter @persistent if true of false
     *
     * @param persistent if it is set to true, save the check point to outside system,
     *                   other wise save it to memory
     */
    void saveCheckPoint(boolean persistent) throws LogHubCheckPointException;

    /**
     * Get the last saved check point
     *
     * @return the last saved checkpoint
     */
    String getCheckPoint();

    /**
     * Get the cursor of current fetched data.
     *
     * @return The cursor of current fetched data.
     */
    String getCurrentCursor();
}
