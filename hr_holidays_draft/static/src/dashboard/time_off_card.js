/* @odoo-module */
import Popover from "web.Popover";

const { Component } = owl;

export class DraftTimeOffCard extends Component {}

DraftTimeOffCard.template = 'hr_holidays_draft.TimeOffCard';
DraftTimeOffCard.props = ['name', 'id', 'data', 'requires_allocation'];

export class DraftTimeOffCardMobile extends DraftTimeOffCard {}

DraftTimeOffCardMobile.template = 'hr_holidays_draft.TimeOffCardMobile';
