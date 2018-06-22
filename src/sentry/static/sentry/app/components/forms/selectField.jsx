import React from 'react';
import PropTypes from 'prop-types';
import styled from 'react-emotion';

import {StyledForm} from './form';
import FormField from './formField';
import SelectControl from './selectControl';

export default class SelectField extends FormField {
  static propTypes = {
    ...FormField.propTypes,
    options: SelectControl.propTypes.options,
    choices: SelectControl.propTypes.choices,
    clearable: SelectControl.propTypes.clearable,
    onChange: PropTypes.func,
  };

  static defaultProps = {
    ...FormField.defaultProps,
    clearable: true,
  };

  getClassName() {
    return '';
  }

  onChange = opt => {
    const value = opt ? opt.value : null;
    this.setValue(value);
  };

  getField() {
    const {options, creatable, choices, placeholder, disabled, required} = this.props;

    return (
      <StyledSelectControl
        creatable={creatable}
        id={this.getId()}
        choices={choices}
        options={options}
        placeholder={placeholder}
        disabled={disabled}
        required={required}
        value={this.state.value}
        onChange={this.onChange.bind(this)}
        clearable={this.props.clearable}
      />
    );
  }
}

// This is to match other fields that are wrapped by a `div.control-group`
const StyledSelectControl = styled(SelectControl)`
  ${StyledForm} &, .form-stacked & {
    .control-group & {
      margin-bottom: 0;
    }

    margin-bottom: 15px;
  }
`;
