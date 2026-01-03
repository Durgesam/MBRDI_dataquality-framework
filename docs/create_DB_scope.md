# Create Secret Scope in Databricks Workspace

Update!!: This step can currently just be done with eXtollo Support. 

1. Login to Azure Portal
2. Get Databricks Instance URL an copy it.
   
   <img src= "img/db_get_databricks_url.png" alt="db_url" width="700"/>
3. Enter the follwing url in your browser by replacing with your copied Databricks instance URL : **databricks-URL**#secrets/createScope
   
   <img src= "img/db_create_scope_empty.png" alt="db_scope_empty" width="400"/>

4. Get your DNS and Resource ID from Azure Key Vault

   <img src= "img/db_get_keyvault_info.png" alt="db_keyvault" width="700"/>

5. Create the scope with a name you like and enter the information copied from step 4 **Vault URL into DNS-Name** and **Resource ID to Resource ID** and make sure that **All Users** is activated under the **Managed Principal** Dropdown

   <img src= "img/db_create_scope_final.png" alt="db_scope_final" width="500"/>

6. Now your Azure Key Vault is linked to your Databricks Secret scope and you can get your credentials with the Azure KeyVault secret names in your notebooks. 


```python
secret = dbutils.secrets.get(scope="<Your Databricks Secret Scope>",key="<Name of secret in Azure Key Vault>")
```