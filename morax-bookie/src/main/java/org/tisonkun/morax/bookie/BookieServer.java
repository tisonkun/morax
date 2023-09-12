package org.tisonkun.morax.bookie;

import org.tisonkun.morax.proto.config.MoraxBookieServerConfig;

public class BookieServer {
    private final Bookie bookie;

    public BookieServer(MoraxBookieServerConfig serverConfig) {
        this.bookie = new Bookie(serverConfig);
    }
}
