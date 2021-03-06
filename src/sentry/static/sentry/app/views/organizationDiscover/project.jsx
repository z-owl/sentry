import React from 'react';
import PropTypes from 'prop-types';
import moment from 'moment';
import styled from 'react-emotion';
import {Flex, Box} from 'grid-emotion';

import DropdownLink from 'app/components/dropdownLink';
import Button from 'app/components/buttons/button';
import MultiSelectField from 'app/components/forms/multiSelectField';
import {t} from 'app/locale';

export default class Project extends React.Component {
  static propTypes = {
    value: PropTypes.array,
    projects: PropTypes.array,
    onChange: PropTypes.func,
    runQuery: PropTypes.func,
  };

  formatDate(date) {
    return moment(date).format('MMMM D, h:mm a');
  }

  render() {
    const {value, projects, onChange, runQuery} = this.props;
    const selectedProjectIds = new Set(value);

    const projectList = projects
      .filter(project => selectedProjectIds.has(parseInt(project.id, 10)))
      .map(project => project.slug);

    const summary = projectList.length
      ? `${projectList.join(', ')}`
      : t('None selected, using all');

    const options = projects.map(project => {
      return {
        value: parseInt(project.id, 10),
        label: project.slug,
      };
    });

    return (
      <StyledProject direction="column" justify="center">
        <label>{t('Projects')}</label>
        <DropdownLink title={summary} keepMenuOpen={true} anchorRight={true}>
          <Box p={2}>
            searched project list
            <MultiSelectField
              name="projects"
              value={value}
              options={options}
              onChange={onChange}
            />
            <Button onClick={runQuery}>{t('Update')}</Button>
          </Box>
        </DropdownLink>
      </StyledProject>
    );
  }
}

const StyledProject = styled(Flex)`
  text-align: right;
  label {
    font-weight: 400;
    font-size: 13px;
    color: #afa3bb;
    margin-bottom: 12px;
  }
  .dropdown-actor-title {
    font-size: 15px;
    height: auto;
    color: ${p => p.theme.button.default.colorActive};
  }
`;
