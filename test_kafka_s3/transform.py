from canonical import Customer, Individual, CustomerStatus, PartyType, Gender, MaritalStatus, Account

def raw_table_cust_transform(payload):

    individual = Individual()
    individual.id = "CUST" + payload.get('cust_num', "")
    individual.name = payload.get('name', None)
    individual.first_name = payload.get('fname', None)
    individual.last_name = payload.get('lname', None)
    individual.prefix = payload.get('form', None)

    gender_map = {'M': Gender.Male, 'F': Gender.Female, 'N': Gender.Nonbinary}
    individual.gender = gender_map[payload['sex']]

    marital_map = {'M': MaritalStatus.Married, 'S': MaritalStatus.Single}
    individual.marital_status = marital_map[payload['maritalstatus']]

    customer = Customer()
    customer.customer_number = payload.get('cust_num', None)
    customer.status = CustomerStatus.Active
    customer.party_id = individual.id
    customer.party_type = PartyType.Individual

    return {"customer": customer.to_dict(), "individual": individual.to_dict()}


def raw_table_acct_transform(payload):
    account = Account()
    account.account_number = payload.get('acct_num', None)
    account.account_name = payload.get('name', None)
    account.account_type = payload.get('type', None)
    account.customer_number = payload.get('cust_num', None)
    account.status = CustomerStatus.Active
    return account.to_dict()
