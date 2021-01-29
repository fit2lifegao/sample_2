import { Mailgun } from '../connector/mailgun'
import { Twilio } from '../connector/twilios'
import { Notifications } from '../../legacy_models/entities/Notifications';
import { DELIVERY_TYPE } from '../common/utils/constants';
import { ENV_PARAMS } from "../environment";
const logger = require('../logger');

/**
 * Base class for all handlers
 */
export class Handler {
    notification: Notifications;

    constructor(notification: Notifications) {
      this.notification = notification;
    }
  
    async notify() {
      throw new Error('`notify() Not Implemented.`');
    }
  }

/**
 * Handler to send notifications via Email 
 */
class EmailHandler extends Handler {
    // Append target url to text body
    _bodyText() {
      const actionUrl = this.notification.onClickUrl;
      let bodyText = this.notification.bodyText || '';
  
      if (actionUrl) {
        bodyText = bodyText + `\n\n${actionUrl}`;
      }
  
      return bodyText;
    }
  
    async notify() {
    // Short-circut for missing email
      if (!this.notification.recipient) {
        return;
      }

      const data = {
        from: ENV_PARAMS['OUTGOING_EMAIL_ADDRESS'],
        to: this.notification.recipient,
        subject: this.notification.subject,
        text: this._bodyText(),
        html: this.notification.bodyHtml
      };

      // Message Id is a custom header that can be used to track
      // quality using Web hooks, it takes a format <ObjectId>@<email_address>
      data['h:Message-Id'] = `${this.notification.id}@${this.notification.recipient}`;
      data['o:tracking-opens'] = 'yes';

      try {
          const mailGun = new Mailgun();
           await mailGun.sendEmail({...data});
      } catch (error) {
        // Sending email failed, log and swallow
        logger.error("Sending email errors:"+error);
      }
    }
  }


  /**
 * Handler to send notifications via SMS
 */
class TextHandler extends Handler {
    async notify() { //TODO

    }
  }
  
/**
 *  Factory to construct the required handler for the delivery type.
 */
export const HandlerFactory = (deliveryType:string=DELIVERY_TYPE.EMAIL, notification:Notifications):Handler => {
    let handler;
  
    switch (deliveryType) {
      case DELIVERY_TYPE.EMAIL:
        handler = new EmailHandler(notification);
        break;
      default:
        throw new Error(`Unknown deliveryType of ${deliveryType}.`);
    }
  
    return handler;
  };
  