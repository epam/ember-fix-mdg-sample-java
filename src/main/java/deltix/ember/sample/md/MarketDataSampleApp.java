package deltix.ember.sample.md;

import quickfix.*;
import quickfix.field.*;
import quickfix.fix50sp2.MarketDataRequest;
import quickfix.fix50sp2.MarketDataSnapshotFullRefresh;
import quickfix.fix50sp2.MarketDataIncrementalRefresh;


public class MarketDataSampleApp {
    public static void main(String[] args) throws ConfigError, InterruptedException {
        Application application = new Application() {
            @Override
            public void onCreate(SessionID sessionId) {}

            @Override
            public void onLogon(SessionID sessionId) {
                System.out.println("Logon - " + sessionId);
                sendMarketDataRequest(sessionId);
            }

            @Override
            public void onLogout(SessionID sessionId) {
                System.out.println("Logout - " + sessionId);
            }

            @Override
            public void toAdmin(Message message, SessionID sessionId) {
                try {
                    if (MsgType.LOGON.equals(message.getHeader().getString(MsgType.FIELD))) {
                        message.setField(new Username("user-goes-here"));      //TODO
                        message.setField(new Password("password-goes-here")); //TODO
                    }
                } catch (FieldNotFound fieldNotFound) {
                    fieldNotFound.printStackTrace();
                }
                System.out.println("Sending admin message: " + message);
            }

            @Override
            public void fromAdmin(Message message, SessionID sessionId) {
                System.out.println("Got admin message: " + message);
            }

            @Override
            public void toApp(Message message, SessionID sessionId) {
                System.out.println("Sending app message: " + message);
            }

            @Override
            public void fromApp(Message message, SessionID sessionId) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
                String msgType = message.getHeader().getString(MsgType.FIELD);
                if (MsgType.MARKET_DATA_SNAPSHOT_FULL_REFRESH.equals(msgType)) {
                    onMarketDataSnapshot ((MarketDataSnapshotFullRefresh) message);
                } else if (MsgType.MARKET_DATA_INCREMENTAL_REFRESH.equals(msgType)) {
                    onMarketDataIncrementalUpdate((MarketDataIncrementalRefresh) message);
                } else {
                    System.err.println("Got other app message: " + message);
                }
            }
        };

        SessionSettings settings = new SessionSettings("src/main/resources/quickfixj.cfg");
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new FileLogFactory(settings);
        MessageFactory messageFactory = new DefaultMessageFactory();
        SocketInitiator initiator = new SocketInitiator(application, storeFactory, settings, logFactory, messageFactory);

        initiator.start();
        // Wait forever
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void sendMarketDataRequest(SessionID sessionId) {
        MarketDataRequest request = new MarketDataRequest();
        request.set(new MDReqID("MDREQ" + System.currentTimeMillis()));
        request.setField(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
        request.setField(new MarketDepth(0)); // 0 = all levels available
        request.setField(new MDUpdateType(MDUpdateType.INCREMENTAL_REFRESH));

        MarketDataRequest.NoMDEntryTypes entryTypes = new MarketDataRequest.NoMDEntryTypes();
        entryTypes.set(new MDEntryType(MDEntryType.BID));
        request.addGroup(entryTypes);
        entryTypes.set(new MDEntryType(MDEntryType.OFFER));
        request.addGroup(entryTypes);
        entryTypes.set(new MDEntryType(MDEntryType.TRADE));
        request.addGroup(entryTypes);

        MarketDataRequest.NoRelatedSym relatedSym = new MarketDataRequest.NoRelatedSym();
        relatedSym.set(new SecurityType(SecurityType.OPTION));
        request.addGroup(relatedSym);

// Alternative
//        MarketDataRequest.NoRelatedSym relatedSym = new MarketDataRequest.NoRelatedSym();
//        relatedSym.set(new Symbol("VALED-0001-C-CT-USD"));
//        request.addGroup(relatedSym);
//        relatedSym.set(new Symbol("SPYD-0001-C-CT-USD"));
//        request.addGroup(relatedSym);
//        relatedSym.set(new Symbol("ZMD-0001-C-CT-USD"));
//        request.addGroup(relatedSym);

        try {
            System.out.println("Sending market data subscription request");
            Session.sendToTarget(request, sessionId);
        } catch (SessionNotFound e) {
            e.printStackTrace();
        }
    }

    private static void onMarketDataSnapshot(MarketDataSnapshotFullRefresh snapshot) {
        try {
            System.out.printf("%s Market Data Snapshot for %s\n", snapshot.getString(TransactTime.FIELD), snapshot.getString(Symbol.FIELD));
            for (Group group : snapshot.getGroups(NoMDEntries.FIELD)) {  // Extract and print bid and ask market data entries
                String mdEntryType = getMDEntryTypeString(group.getChar(MDEntryType.FIELD));
                double mdEntryPx = group.getDouble(MDEntryPx.FIELD);
                double mdEntrySize = group.getDouble(MDEntrySize.FIELD);

                System.out.println("\tMDEntryType: " + mdEntryType + ", MDEntryPx: " + mdEntryPx + ", MDEntrySize: " + mdEntrySize);
            }
        } catch (FieldNotFound e) {
            System.err.printf("Missing tag %s in message %s", e.field, snapshot);
        }
    }

    private static void onMarketDataIncrementalUpdate(MarketDataIncrementalRefresh message) {
        try {
            System.out.printf("%s Market Data Increment for %s\n", message.getString(TransactTime.FIELD), message.getString(Symbol.FIELD));

            for (Group group : message.getGroups(NoMDEntries.FIELD)) {
                String mdUpdateAction = getMDUpdateActionString(group.getChar(MDUpdateAction.FIELD));
                String mdEntryType = getMDEntryTypeString(group.getChar(MDEntryType.FIELD));
                double mdEntryPx = group.getDouble(MDEntryPx.FIELD);
                double mdEntrySize = group.getDouble(MDEntrySize.FIELD);

                System.out.println("\tMDUpdateAction: " + mdUpdateAction + ", MDEntryType: " + mdEntryType + ", MDEntryPx: " + mdEntryPx + ", MDEntrySize: " + mdEntrySize);
            }
        } catch (FieldNotFound e) {
            System.err.printf("Missing tag %s in message %s", e.field, message);
        }
    }

    // Helper methods to get human-readable strings for enum values
    private static String getMDEntryTypeString(char value) {
        switch (value) {
            case MDEntryType.BID: return "Bid";
            case MDEntryType.OFFER: return "Offer";
            case MDEntryType.TRADE: return "Trade";
            case MDEntryType.INDEX_VALUE: return "Index Value";
            case MDEntryType.OPENING_PRICE: return "Opening Price";
            case MDEntryType.CLOSING_PRICE: return "Closing Price";
            case MDEntryType.SETTLEMENT_PRICE: return "Settlement Price";
            case MDEntryType.TRADING_SESSION_HIGH_PRICE: return "Trading Session High Price";
            case MDEntryType.TRADING_SESSION_LOW_PRICE: return "Trading Session Low Price";
            case MDEntryType.TRADING_SESSION_VWAP_PRICE: return "Trading Session VWAP Price";
            case MDEntryType.IMBALANCE: return "Imbalance";
            case MDEntryType.TRADE_VOLUME: return "Trade Volume";
            case MDEntryType.OPEN_INTEREST: return "Open Interest";
            // Add more cases as needed
            default: return "Other";
        }
    }

    private static String getMDUpdateActionString(char value) {
        switch (value) {
            case MDUpdateAction.NEW: return "New";
            case MDUpdateAction.CHANGE: return "Change";
            case MDUpdateAction.DELETE: return "Delete";
            case MDUpdateAction.DELETE_THRU: return "Delete Thru";
            case MDUpdateAction.DELETE_FROM: return "Delete From";
            // Add more cases as needed
            default: return "Unknown";
        }
    }


}
