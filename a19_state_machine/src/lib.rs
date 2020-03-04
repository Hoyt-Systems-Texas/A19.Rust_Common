pub trait StatePersisted<EVT, KEY, STATE, MODEL, MSG, RESULT> {

    /// The state this node if for.
    fn state(&self) -> STATE;

    /// The list of events this node handles.
    /// #Arguments
    /// `act` - The action that adds the events for the state machine.
    fn events<F>(&self, act: F) where F:FnMut(EventAction<EVT, KEY, STATE, MODEL, MSG, RESULT>);

    /// The entry for the state.
    /// #Arguments
    /// `evt` - The event we are enterying.
    /// `ctx` - The mutable context.
    /// `msg` - The message we are processing.
    fn entry(&self, evt: &EVT, ctx: &mut Context<KEY, STATE, MODEL>, msg: &MSG);

    /// Called when we exit a state.
    /// # Arguments
    /// `evt` - The event we are processing.
    /// `ctx` - The mutable context for the state transition.
    /// `msg` - The message we are processing.
    fn exit(&self, evt: &EVT, ctx: &mut Context<KEY, STATE, MODEL>, msg: &MSG);
}

pub struct Context<KEY, STATE, MODEL> {
    id: KEY,
    current_state: STATE,
    model: MODEL,
    last_transition_id: u64,
}

impl<KEY, STATE, MODEL> Context<KEY, STATE, MODEL> {

    pub fn new(id: KEY, current_state: STATE, model: MODEL, last_transition_id: u64) -> Self {
	let context = Context {
	    id,
	    current_state, 
	    model,
	    last_transition_id,
	};
	context
    }

    pub fn id(&self) -> &KEY {
	&self.id
    }

    /// Sets the state of the model.
    pub fn set_state(&mut self, state: STATE) {
	self.current_state = state;
    }

    /// Gets the model for edditing.
    pub fn model<'a>(&'a mut self) -> &'a mut MODEL {
	&mut self.model
    }

    /// Gets the last transition id.
    pub fn last_transition_id(&self) -> &u64 {
	&self.last_transition_id
    }

    pub fn set_last_transition_id(&mut self, id: u64) {
	self.last_transition_id = id;
    }

    /// Gets the current state of the context.
    pub fn state(&self) -> &STATE {
	&self.current_state
    }
}

pub trait Action<KEY, STATE, MODEL, MSG, RESULT> {

    /// Used to run the specified action with the ctx and messag.
    /// # Arguments
    /// `ctx` - The context for the state machine.
    /// `msg` - The message for the state machine.
    fn run(&self, ctx: &mut Context<KEY, STATE, MODEL>, msg: &MSG) -> RESULT;
}

pub enum EventAction<EVT, KEY, STATE, MODEL, MSG, RESULT> {
    GoToState{event: EVT, state: STATE},
    DoAction{event: EVT, action: Box<dyn Action<KEY, STATE, MODEL, MSG, RESULT>>},
    Ignore{event: EVT},
    Defer{event: EVT},
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
