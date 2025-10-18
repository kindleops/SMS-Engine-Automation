from sms.ai import autoresponder, comps_engine, offer_engine, contract_engine, followup_engine


class SMSPipeline:
    def __init__(self, lead_id, phone, market, property_address=None):
        self.lead_id = lead_id
        self.phone = phone
        self.market = market
        self.property_address = property_address
        self.state = "DISCOVERY"

    def handle_inbound(self, message: str):
        """
        Route inbound seller SMS into correct AI module.
        """
        if self.state == "DISCOVERY":
            response, ready_for_comps = autoresponder.handle(message)
            if ready_for_comps:
                self.state = "COMPS"
            return response

        elif self.state == "COMPS":
            arv, repairs = comps_engine.run(self.property_address)
            offer_price = offer_engine.calculate(arv, repairs)
            self.state = "OFFER"
            return f"Based on sales nearby, your home looks around ${arv:,.0f}. Accounting for repairs, we could do about ${offer_price:,.0f}. Does that work?"

        elif self.state == "OFFER":
            counter, accepted = offer_engine.negotiate(message)
            if accepted:
                self.state = "CONTRACT"
                return "Great — I’ll prepare the purchase agreement and send it securely for review."
            else:
                return counter

        elif self.state == "CONTRACT":
            contract_link = contract_engine.send_contract(
                seller_name="Seller", seller_email="seller@email.com", address=self.property_address, offer_price=offer_engine.last_offer
            )
            self.state = "CLOSED"
            return f"Here’s your contract link: {contract_link}"

        elif self.state == "FOLLOWUP":
            return followup_engine.reengage(self.phone)

        return "⚠️ Unexpected state, resetting conversation."
