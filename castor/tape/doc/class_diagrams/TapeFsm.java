class AbstractCallback {
}

/**
 * @hidden
 */
class Dummy {
}

/**
 * @opt attributes
 */
class Callback extends AbstractCallback {
  Dummy m_object;
  Dummy m_pointerToMemberFunction;
}


/**
 * @opt attributes
 * @composed * - 1 AbstractCallback
 */
class Transition {
  String fromState;
  String toState;
  String event;
}


/**
 * @has 1 - * Transition
 */
class StateMachine {
}

/**
 * @composed 1 - * StateMachine
 */
class Application {
}
