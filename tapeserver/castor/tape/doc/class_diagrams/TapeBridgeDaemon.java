/**
 */
class BaseDaemon {
}


/**
 * @assoc 1 - 1 BridgeProtocolEngine
 * @opt operations
 */
class VdqmRequestHandler {
public void run();
}


/**
 * @composed 1 - 1 VdqmRequestHandler
 */
class TCPListenerThreadPool {
}


/**
 * @opt operations
 */
class BridgeProtocolEngine {
public void run();
}


/**
 * @composed 1 - 1 TCPListenerThreadPool
 */
class TapeBridgeDaemon extends BaseDaemon {
}
