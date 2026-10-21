#!/usr/bin/env python3
"""Provision keyless K3s identities scoped to BDC warehouse resources.

Requires an authorized operator's Google CLI login and the cluster's public OIDC
issuer/JWKS files. No private key, user credential, or runtime token is exported.
"""
import argparse
import json
import shutil
import subprocess
from pathlib import Path

from warehouse import GoogleAPI, ROOT, load_config

POOL='bdc-k3s'
PROVIDER='warehouse'


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--issuer',type=Path,required=True)
    p.add_argument('--jwks',type=Path,required=True)
    p.add_argument('--gcloud-bin',default=shutil.which('gcloud'))
    args=p.parse_args()
    config=load_config(ROOT/'config/project.json')
    api=GoogleAPI(config,args.gcloud_bin)
    api.inspect()
    project=config['project_id']
    def cloud(*parts):
        r=subprocess.run([args.gcloud_bin,*parts,f'--project={project}','--quiet','--format=json'],capture_output=True,text=True,timeout=180)
        if r.returncode:
            # Only CLI error headers are emitted, not policy contents or tokens.
            reason=next((line for line in r.stderr.splitlines() if line.startswith('ERROR:')), 'Google CLI failed')
            raise RuntimeError(reason)
        return json.loads(r.stdout) if r.stdout.strip() else None
    cloud('services','enable','iam.googleapis.com','iamcredentials.googleapis.com','sts.googleapis.com')
    project_meta=cloud('projects','describe',project)
    number=str(project_meta['projectNumber'])
    issuer=json.loads(args.issuer.read_text())['issuer']
    jwks=json.loads(args.jwks.read_text())
    if not jwks.get('keys') or not issuer.startswith('https://'):
        raise RuntimeError('Invalid public cluster identity metadata')
    pool=f'projects/{number}/locations/global/workloadIdentityPools/{POOL}'
    provider=pool+f'/providers/{PROVIDER}'
    audience='https://iam.googleapis.com/'+provider
    if api.request('https://iam.googleapis.com/v1/'+pool,allow_missing=True) is None:
        cloud('iam','workload-identity-pools','create',POOL,'--location=global','--display-name=BDC K3s warehouse')
    found=api.request('https://iam.googleapis.com/v1/'+provider,allow_missing=True)
    if found and found.get('oidc',{}).get('issuerUri')!=issuer:
        raise RuntimeError('Existing provider belongs to another issuer; no trust was changed')
    subjects=['system:serviceaccount:default:data-warehouse','system:serviceaccount:default:data-warehouse-deployer']
    operation='update-oidc' if found else 'create-oidc'
    cloud('iam','workload-identity-pools','providers',operation,PROVIDER,
          '--location=global',f'--workload-identity-pool={POOL}',f'--issuer-uri={issuer}',
          f'--jwk-json-path={args.jwks.resolve()}',f'--allowed-audiences={audience}',
          '--attribute-mapping=google.subject=assertion.sub',
          '--attribute-condition=assertion.sub in '+json.dumps(subjects))
    for account,ksa,layers in [
        ('bdc-warehouse-ingest','data-warehouse',['raw']),
        ('bdc-warehouse-deploy','data-warehouse-deployer',['raw','staging','core','mart','governance']),
    ]:
        email=f'{account}@{project}.iam.gserviceaccount.com'
        sa_url=f'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{email}'
        if api.request(sa_url,allow_missing=True) is None:
            cloud('iam','service-accounts','create',account,f'--display-name={ksa}')
        cloud('projects','add-iam-policy-binding',project,f'--member=serviceAccount:{email}',
              '--role=roles/bigquery.jobUser','--condition=None')
        subject=f'system:serviceaccount:default:{ksa}'
        principal=f'principal://iam.googleapis.com/{pool}/subject/{subject}'
        cloud('iam','service-accounts','add-iam-policy-binding',email,
              f'--member={principal}','--role=roles/iam.workloadIdentityUser','--condition=None')
        for layer in layers:
            ds=f'bdc_{layer}_{config["environment"]}'
            url=f'{api.bq}/datasets/{ds}'
            current=api.request(url)
            access=current.get('access',[])
            entry={'role':'WRITER','userByEmail':email}
            if entry not in access:
                api.request(url,{'access':access+[entry]},method='PATCH',extra_headers={'If-Match':current['etag']})
        credential={
            'type':'external_account',
            'audience':'//iam.googleapis.com/'+provider,
            'subject_token_type':'urn:ietf:params:oauth:token-type:jwt',
            'token_url':'https://sts.googleapis.com/v1/token',
            'service_account_impersonation_url':f'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{email}:generateAccessToken',
            'credential_source':{'file':'/var/run/gcp-identity/token','format':{'type':'text'}},
        }
        (ROOT/f'config/{ksa}-identity.json').write_text(json.dumps(credential,indent=2)+'\n')
    (ROOT/'config/identity-metadata.json').write_text(json.dumps({
        'project_number':number,'provider':provider,'token_audience':audience,
    },indent=2)+'\n')
    print(json.dumps({'workload_identity_configured':True,'keys_created':False,'billing_changed':False,'service_accounts':2}))


if __name__=='__main__':
    main()
