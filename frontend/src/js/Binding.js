/**
 * Lightweight data-binding
 * @module
 */

const identity = (x) => x;

/**
 * A BoundVariable object has a value that can be bound to a DOM elelment
 *   or other arbitrary obejct.  It is used to sync properties between
 *   two or more objects.  It consists of a getter and setter for the value
 *   as well as one or more two-way bindings.
 */
export class BoundVariable {
  /**
   * @param value An initial value
   */
  constructor(value) {
    this.DOMbindings = [];
    this.countDB = 0;

    this.generalBindings = [];
    this.countGB = 0;

    this._value = value;
  }

  /**
   * @return The current value of this variable
   */
  get() {
    return this._value;
  }

  /**
   * Set a new value for this variable
   * @param newValue the new value
   */
  set(newValue) {
    this._value = newValue;
    // console.log(newValue);

    for (let i = 0; i < this.countDB; i++) {
      const binding = this.DOMbindings[i];
      binding.element[binding.attribute] = binding.format(newValue);
    }

    for (let i = 0; i < this.countGB; i++) {
      const binding = this.generalBindings[i];
      binding.setFunc(binding.format(newValue));
    }
  }

  /**
   * Sets the property "value" as a getter.
   * @return the current value of this variable
   */
  get value() {
    return this._value;
  }

  /**
   * Sets the property "value" as a setter.
   * @param  {[type]} newValue the new value
   */
  set value(newValue) {
    this.set(newValue);
  }

  /**
   * Sync this value with an attribute of a DOM element.
   *
   * @param {Object} args - keyword arguments in an Object
   * @param {HTMLElement} args.element - The DOM element
   * @param {String} [args.attribute="value"] - The attribute to sync
   *                 (eg. "value", "checked", "href", "innerHTML", etc.)
   * @param {String} [args.event] - The event on which to update the value
   *         (eg. "change", "keyup", etc). "none" is the same as not having an event.
   * @param {function} [args.DOMformat] - A function that converts the varible value
   *                                    to be displayed in the DOM.
   * @param {function} [args.varFormat] - A function that converts the DOM value to
   *                                    the variable value.
   */
  addDOMbinding({ element, attribute, DOMformat, event, varFormat }) {
    const binding = {
      element: element,
      attribute: attribute,
      format: DOMformat || identity,
    };

    if (event && event !== "none") {
      const format = varFormat || identity;

      element.addEventListener(
        event,
        function () {
          this.set(format(element[attribute]));
        }.bind(this)
      );
    }

    this.DOMbindings.push(binding);
    this.countDB = this.DOMbindings.length;

    element[attribute] = this._value;

    return this;
  }

  /**
   * Add binding to a generally defined remote value.
   * Use this to sync this value with a general variable, given a
   * function to update that variable. !! The user is responsible for
   * updating this variable when the remote value changes !!
   *
   * @param {function} setFunc - A function that updates the remote value
   *                           when this one changes.
   */
  addGeneralBinding(setFunc) {
    this.generalBindings.push(setFunc);
    this.countGB = this.generalBindings.length;

    setFunc(this._value);

    return this
  }
}

/**
 * An object with {@link BoundVariable}s as properties.
 * The value of a {@link BoundVariable} x is accessed
 *  via x.value (the "value" property)
 *  or using accessor methods x.get() and x.set().
 *
 *  {@link BoundVariableCollection} allows us to keep a collection of
 *    {@link BoundVariable} objects and access them as if they were properties.
 *  For example, for col = new {@link BoundVariableCollection} and
 *  col.add("size", new {@link BoundVariable}(23)):
 *
 *  @example col.size == 23
 *
 *
 */
export class BoundVariableCollection extends Object {
  constructor(...args) {
    super(...args);
    this.binds = {};
  }

  /**
   * Add a {@link BoundVariable} object
   * @param {String|Number} key The "property" associated with this {@BoundVariable}
   * @param {[type]} bv The {@link BoundVariable} object.
   */
  add(key, bv) {
    this.binds[key] = bv;

    Object.defineProperty(this, "binds", {
      enumerable: false,
    });

    Object.defineProperty(this, key, {
      get: function () {
        return this.binds[key].get();
      }.bind(this),
      set: function (newValue) {
        this.binds[key].set(newValue);
      }.bind(this),

      enumerable: true,
    });

    return bv;
  }
}
