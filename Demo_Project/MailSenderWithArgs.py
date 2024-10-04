import requests
import sys

def send_email(client_id, client_secret, tenant_id, user_email, subject, message):

    recipient = []
    content = ""
    if message == 'fmc' :
        print('fmc')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Fact MemberCoverage and BI MemberCoverage Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>In Progress</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Pending</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Pending</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Pending</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Pending</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Pending</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Pending</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Pending</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Pending</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Pending</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Pending</td> </tr> </table>'
    elif message == 'factclaims' :
        print('factclaims')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Fact Claims and Provider Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Complete</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Complete</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>In Progress</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Pending</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Pending</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Pending</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Pending</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Pending</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Pending</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Pending</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Pending</td> </tr> </table>'
    elif message == 'pharmacyclaims' : 
        print('pharmacyclaims')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : EDW Reference Tables,Pharmacy Claims and Fact Rx Claims Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Complete</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Complete</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Complete</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Complete</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Complete</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>In Progress</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Pending</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Pending</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Pending</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Pending</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Pending</td> </tr> </table>'
    elif message == 'biclaims' :
        print('biclaims')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Bi Facility and Bi Professional Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Complete</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Complete</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Complete</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Complete</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Complete</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Complete</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Complete</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>In Process</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Pending</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Pending</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Pending</td> </tr> </table>'
    elif message == 'cotivity' :
        print('cotivity')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Cotiviti BI Claims and Cotiviti BI Eligibility Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Complete</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Complete</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Complete</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Complete</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Complete</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Complete</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Complete</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Complete</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Complete</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>In Progress</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Pending</td> </tr> </table>'
    elif message == 'millman' :
        print('millman')
        recipient = ["Jeff.Kwon@1199funds.org","Elizabeth.Braley@1199funds.org","Anna.Kozelkevich@1199funds.org","Kumar.Pandruvada@1199funds.org","jgopireddy@1199nbf.net","ntumburu@1199nbf.net","teja.veturi@1199funds.org","marlon.hay@1199funds.org","dmude@1199nbf.net","nchalampalem@1199nbf.net","vmudireddy@1199nbf.net","rdharmavaram@1199nbf.net","spendyala@1199nbf.net","savula@1199nbf.net","samirisetty@1199nbf.net","anusha.mulabagula@1199funds.org","zahirbasha.shaikazeez@1199funds.org","hkakarla@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Millman Grouper and MedClaims Modules are completed</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Complete</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Complete</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Complete</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Complete</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Complete</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Complete</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Complete</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Complete</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Complete</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Complete</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Complete</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Complete</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Complete</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Complete</td> </tr> </table>'
    else :
        print('inside test case...')
        recipient = ["jgopireddy@1199nbf.net","dmude@1199nbf.net","nchalampalem@1199nbf.net","ntumburu@1199nbf.net"]
        content = '<h5>BigQuery Monthly Production Load Status : Test Mail</h5> <table> <style>table , th, td{border: 1px solid black;border-collapse: collapse;}</style> <tr> <th></th> <th scope="col">Process Name</th> <th scope="col">Load Status</th> </tr> <tr> <td>1</td> <td>Dimensions&Lookups</td> <td>Testing</td> </tr> <tr> <td>2</td> <td>Fact Eligibility</td> <td>Testing</td> </tr> <tr> <td>3</td> <td>Fact_Member_Coverage&BI_Member_Coverage</td> <td>Testing</td> </tr> <tr> <td>4</td> <td>Provider Module</td> <td>Testing</td> </tr> <tr> <td>5</td> <td>Fact_Claims</td> <td>Testing</td> </tr> <tr> <td>6</td> <td>EDW Reference Tables</td> <td>Testing</td> </tr> <tr> <td>7</td> <td>Fact_Rx_Claims</td> <td>Testing</td> </tr> <tr> <td>8</td> <td>Pharmacy_Claims</td> <td>Testing</td> </tr> <tr> <td>9</td> <td>BI_Professional_Claims</td> <td>Testing</td> </tr> <tr> <td>10</td> <td>BI_Facility_Paid_Claim</td> <td>Testing</td> </tr> <tr> <td>11</td> <td>Cotiviti_BI_Claims</td> <td>Testing</td> </tr> <tr> <td>12</td> <td>Cotiviti_BI_Eligibility</td> <td>Testing</td> </tr> <tr> <td>13</td> <td>Milliman Grouper</td> <td>Testing</td> </tr> <tr> <td>14</td> <td>Med claims</td> <td>Testing</td> </tr> </table>'
    
    print(recipient)
        
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }

    token_response = requests.post(token_url, data=token_data)
    token_response.raise_for_status()
    access_token = token_response.json()['access_token']
    #print(access_token)

    send_mail_url = f'https://graph.microsoft.com/v1.0/users/{user_email}/sendMail'

    message = {
        'message': {
            'subject': subject,
            'body': {
                'contentType': 'HTML',
                'content': content
            },            
            "toRecipients": [
            {"emailAddress": {"address": recipient_email}} for recipient_email in recipient
        ]
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    send_response = requests.post(send_mail_url, json=message, headers=headers)
    send_response.raise_for_status()

    if send_response.ok:
        print('Email sent successfully.')
    else:
        print('Error sending email:')
        print(send_response.json())

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python script.py <client_id> <client_secret> <tenant_id> <user_email> <subject> <message>")
        sys.exit(1)

    client_id, client_secret, tenant_id, user_email, subject, message = sys.argv[1:]
    send_email(client_id, client_secret, tenant_id, user_email, subject, message)