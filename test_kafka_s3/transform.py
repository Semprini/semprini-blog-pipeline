from canonical import Customer, Individual, CustomerStatus, PartyType

def raw_table_cust_transform(payload):
    customer = Customer()
    individual = Individual()

    individual.id = "CUST" + payload.get('cust_num', "")
    individual.name = payload.get('name', None)
    
    customer.customer_number = payload.get('cust_num', None)
    customer.status = CustomerStatus.Active
    customer.party_id = individual.id
    customer.party_type = PartyType.Individual

    return {"customer": customer.to_dict(), "individual": individual.to_dict()}


def raw_table_acct_transform(payload):
    transformed = {}
    return transformed
