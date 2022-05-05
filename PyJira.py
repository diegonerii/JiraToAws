import pandas as pd
import boto3
import requests
from io import StringIO

def lambda_handler(event, context):
    # https://docs.atlassian.com/software/jira/docs/api/REST/8.20.2/
    df_credentials = pd.read_excel('~\credenciais.xlsx)
    AccessKeyId = df_credentials.iloc[0,0]
    SecretKey = df_credentials.iloc[1,0]
    bucketname = df_credentials.iloc[2,0]
    region = df_credentials.iloc[3,0]
    token_lyzh = df_credentials.iloc[4,0]
    domain = df_credentials.iloc[5,0]
    
    def append_valor(lista, campo_json):
        try:
            lista.append(campo_json)
        except:
            lista.append('') 
        return
    
    tipo_issue = []
    key_issue = []
    id_issue = []
    titulo_issue = []
    assignee_issue = []
    reporter_issue = []
    prioridade_issue = []
    status_issue = []
    resolution_issue = []
    created_date_issue = []
    updated_date_issue = []
    due_date_issue = []
    fix_versions0_issue = []
    fix_versions1_issue = []
    fix_versions2_issue = []
    fix_versions3_issue = []
    ed_end_date_issue = []
    ed_start_date_issue = []
    cf_start_date_issue = []
    cf_flagged = []
    project_key_issue = []
    project_name_issue = []
    project_typekey_issue = []
    project_lead = []
    project_description = []
    project_url = []
    cf_area = []
    components_issue = []
    components_issue1 = []
    components_issue2 = []
    cf_epic_link = []
    cf_epic_name = []
    cf_reference = []
    label0_issue = []
    label1_issue = []
    label2_issue = []
    label3_issue = []
    label4_issue = []
    label5_issue = []
    sprint = []
    sprint1 = []
    description_issue = []
    cf_begin_date_issue = []
    cf_end_date_issue = []
    cf_end_date_issue1 = []
    
    projetos = ['BEFO', 'BESS', 'BEFE', 'BAFI', 'BECD']
    url_original = "https://{}/rest/api/2/search?jql=project=".format(domain)
    
    for projeto in projetos:
        url = url_original+projeto
        headers = {
           "Content-Type": "application/json",
           "Accept":"application/json",
           "Authorization": "Bearer {}".format(token_lyzh)
        }
        
        response = requests.get(url, headers=headers)
        df = pd.DataFrame(response.json())
        issues = list(df['issues'])
    
        for x in issues:
            try:
                append_valor(tipo_issue, x['fields']['issuetype']['name'])
            except:
                tipo_issue.append('')
            
            try:
                append_valor(key_issue, x['key'])
            except:
                key_issue.append('')
            
            try:
                append_valor(id_issue, x['id'])
            except:
                id_issue.append('')
            
            try:
                append_valor(titulo_issue, x['fields']['summary'])
            except:
                titulo_issue.append('')
                
            try:
                append_valor(assignee_issue, x['fields']['assignee']['name'])
            except:
                assignee_issue.append('')
                
            try:
                append_valor(reporter_issue, x['fields']['reporter']['name'])
            except:
                reporter_issue.append('')
                
            try:
                append_valor(prioridade_issue, x['fields']['priority']['name'])
            except:
                prioridade_issue.append('')
                
            try:
                append_valor(status_issue, x['fields']['status']['name'])
            except:
                status_issue.append('')
    
            try:
                append_valor(resolution_issue, x['fields']['resolution']['name'])
            except:
                resolution_issue.append('')
                
            try:
                append_valor(created_date_issue, x['fields']['created'][:10])
            except:
                created_date_issue.append('')
            
            try:
                append_valor(updated_date_issue, x['fields']['updated'][:10])
            except:
                updated_date_issue.append('')
                
            try:
                append_valor(due_date_issue, x['fields']['duedate'])
            except:
                due_date_issue.append('')
                
            try:
                append_valor(fix_versions0_issue, x['fields']['fixVersions'])
            except:
                fix_versions0_issue.append('')
            fix_versions1_issue.append('')
            fix_versions2_issue.append('')
            fix_versions3_issue.append('')
                
            try:
                append_valor(ed_end_date_issue, x['fields']['resolutiondate'])
            except:
                ed_end_date_issue.append('')
                
            try:
                append_valor(ed_start_date_issue, x['fields']['customfield_11204'])
            except:
                ed_start_date_issue.append('')
                
            try:
                append_valor(cf_start_date_issue, x['fields']['customfield_11207'])
            except:
                cf_start_date_issue.append('')
    
            try:
                append_valor(cf_flagged, x['fields']['customfield_10200'][0]['value'])
            except:
                cf_flagged.append('')
            
            try:
                append_valor(project_key_issue, x['key'][:4])
            except:
                project_key_issue.append('')
                
            try:
                append_valor(project_name_issue, x['fields']['project']['name'])
            except:
                project_name_issue.append('')
                
            try:
                append_valor(project_typekey_issue, x['fields']['project']['projectTypeKey'])
            except:
                project_typekey_issue.append('')
            
            project_lead.append('')
            project_description.append('')
            project_url.append('')
            cf_area.append('')
            
            try:
                append_valor(components_issue, x['fields']['components'])
            except:
                components_issue.append('')
            
            components_issue1.append('')
            components_issue2.append('')
            cf_epic_link.append('')
            cf_epic_name.append('')
            cf_reference.append('')
                
            try:
                append_valor(label0_issue, x['fields']['labels'])
            except:
                label0_issue.append('')
                
            label1_issue.append('')
            label2_issue.append('')
            label3_issue.append('')
            label4_issue.append('')
            label5_issue.append('')
            sprint.append('')
            sprint1.append('')
                
            try:
                append_valor(description_issue, x['fields']['description'])
            except:
                description_issue.append('')
            
            cf_begin_date_issue.append('')
                
            try:
                append_valor(cf_end_date_issue, x['fields']['customfield_11208'])
            except:
                cf_end_date_issue.append('')
            
            cf_end_date_issue1.append('')
        
    dict_field = {
        "Issue Type": tipo_issue,
        "Issue key": key_issue,
        "Issue id": id_issue,
        "Summary": titulo_issue,
        "Assignee": assignee_issue,
        "Reporter": reporter_issue,
        "Priority": prioridade_issue,
        "Status": status_issue,
        "Resolution": resolution_issue,
        "Created": created_date_issue,
        "Updated": updated_date_issue,
        "Due Date": due_date_issue,
        "Fix Version/s": fix_versions0_issue,
        "Fix Version/s1": fix_versions1_issue,
        "Fix Version/s2": fix_versions2_issue,
        "Fix Version/s3": fix_versions3_issue,
        "Custom field (ED end date)": ed_end_date_issue,
        "Custom field (ED start date)": ed_start_date_issue,
        "Custom field (Start date)": cf_start_date_issue,
        "Custom field (Flagged)": cf_flagged,
        "Project key": project_key_issue,
        "Project name": project_name_issue,
        "Project type": project_typekey_issue,
        "Project lead": project_lead,
        "Project description": project_description,
        "Project url": project_url,
        "Custom field (Area)": cf_area,
        "Component/s": components_issue,
        "Component/s1": components_issue1,
        "Component/s2": components_issue2,
        "Custom field (Epic Link)": cf_epic_link,
        "Custom field (Epic Name)": cf_epic_name,
        "Custom field (Reference)": cf_reference,
        "Labels": label0_issue,
        "Labels1": label1_issue,
        "Labels2": label2_issue,
        "Labels3": label3_issue,
        "Labels4": label4_issue,
        "Labels5": label5_issue,
        "Sprint": sprint,
        "Sprint1": sprint1,
        "Description": description_issue,
        "Custom field (Begin Date)": cf_begin_date_issue,
        "Custom field (End date)": cf_end_date_issue,
        "Custom field (End date)1": cf_end_date_issue1
    }
        
    df_final = pd.DataFrame.from_dict(dict_field)
    
    try:
        csv_buffer = StringIO()
        df_final.to_csv(csv_buffer, sep=";", index=False)

        s3_resource = boto3.resource('s3', aws_access_key_id=AccessKeyId, aws_secret_access_key=SecretKey)
        s3_resource.Object(bucketname, 'Jira.csv').put(Body=csv_buffer.getvalue())
 
        print("Salvou o arquivo no bucket {}".format(bucketname))
    except:
        print("Não salvou o arquivo no bucket {}".format(bucketname))
        pass
    print("Fim do Programa")
