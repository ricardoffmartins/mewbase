package io.mewbase.server;

/**
 * Created by tim on 10/01/17.
 */
public interface CommandHandler {

    String getName();

    void pause();

    void resume();

}
