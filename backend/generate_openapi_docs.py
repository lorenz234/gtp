import os
import boto3
import logging
from pydantic.json_schema import models_json_schema

# --- Imports from your project ---
from src.api.schemas import (
    MetricResponse, ChainOverviewResponse, StreaksResponse, 
    EcosystemResponse, UserInsightsResponse
)
from src.misc.helper_functions import upload_json_to_cf_s3

# --- Configuration ---
# You can load these from os.environ or your config files
S3_BUCKET = os.getenv("S3_CF_BUCKET") 
CF_DISTRIBUTION_ID = os.getenv("CF_DISTRIBUTION_ID")
API_VERSION = "v1"
API_BASE_URL = f"https://api.growthepie.com/{API_VERSION}"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_openapi_spec():
    """Generates the OpenAPI 3.0 dictionary."""
    
    # 1. Define the Skeleton
    openapi = {
        "openapi": "3.0.3",
        "info": {
            "title": "growthepie API",
            "version": "1.0.0",
            "description": "Static JSON files served via CloudFront/S3. Data is refreshed periodically."
        },
        "servers": [{"url": API_BASE_URL}],
        "components": {"schemas": {}},
        "paths": {}
    }

    # 2. Extract Schemas from Pydantic
    models = [
        (MetricResponse, "MetricResponse"),
        (ChainOverviewResponse, "ChainOverviewResponse"),
        (StreaksResponse, "StreaksResponse"),
        (EcosystemResponse, "EcosystemResponse"),
        (UserInsightsResponse, "UserInsightsResponse")
    ]
    
    # Generate JSON schemas
    _, top_level_schema = models_json_schema(
        [(m, 'validation') for m, _ in models], 
        ref_template="#/components/schemas/{model}"
    )
    openapi["components"]["schemas"] = top_level_schema.get("$defs", {})

    # 3. Define Paths
    def create_get_op(schema_name, summary):
        return {
            "get": {
                "summary": summary,
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{schema_name}"}}}
                    }
                }
            }
        }

    openapi["paths"]["/metrics/{level}/{origin_key}/{metric_id}"] = create_get_op("MetricResponse", "Get specific metric details")
    openapi["paths"]["/chains/{origin_key}/overview"] = create_get_op("ChainOverviewResponse", "Get chain overview")
    openapi["paths"]["/chains/all/streaks_today"] = create_get_op("StreaksResponse", "Get daily streaks")
    openapi["paths"]["/ecosystem/builders"] = create_get_op("EcosystemResponse", "Get ecosystem apps")
    openapi["paths"]["/chains/{origin_key}/user_insights"] = create_get_op("UserInsightsResponse", "Get user insights")

    return openapi

def get_swagger_html():
    """Returns the HTML string for Swagger UI."""
    # Note: url is set to '../openapi.json' because this HTML lives at /v1/docs/index.html
    # and the JSON lives at /v1/openapi.json
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <title>growthepie API Docs</title>
      <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css" />
      <style>
        body { margin: 0; padding: 0; }
        .swagger-ui .topbar { display: none; } /* Optional: Hide Swagger Bar */
      </style>
    </head>
    <body>
      <div id="swagger-ui"></div>
      <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js" crossorigin></script>
      <script>
        window.onload = () => {
          window.ui = SwaggerUIBundle({
            url: 'openapi.json', 
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
              SwaggerUIBundle.presets.apis,
              SwaggerUIBundle.SwaggerUIStandalonePreset
            ]
          });
        };
      </script>
    </body>
    </html>
    """

def upload_html_to_s3(bucket: str, key: str, html_content: str, dist_id: str):
    """Directly uploads HTML content to S3 using boto3."""
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=html_content,
            ContentType="text/html",
            CacheControl="no-cache, no-store, must-revalidate" # Docs should always be fresh
        )
        logging.info(f"✅ HTML uploaded to s3://{bucket}/{key}")
    except Exception as e:
        logging.error(f"❌ Failed to upload HTML: {e}")

def main():
    """Main function to generate and upload OpenAPI docs."""
    # 1. Generate & Upload OpenAPI JSON
    logging.info("Generating OpenAPI Spec...")
    openapi_spec = generate_openapi_spec()
    
    json_path = f"{API_VERSION}/openapi"
    logging.info(f"Uploading Spec to {json_path}...")
    
    # Using your existing helper for JSON
    upload_json_to_cf_s3(
        bucket=S3_BUCKET,
        path_name=json_path,
        details_dict=openapi_spec,
        cf_distribution_id=CF_DISTRIBUTION_ID,
        invalidate=False # We will batch invalidate at the end
    )
    
    # 2. Generate & Upload HTML
    logging.info("Generating Swagger UI HTML...")
    html_content = get_swagger_html()
    
    docs_path = f"{API_VERSION}/api-docs.html"
    logging.info(f"Uploading Docs to {docs_path}...")
    
    upload_html_to_s3(S3_BUCKET, docs_path, html_content, CF_DISTRIBUTION_ID)
    
    # 3. Invalidate CloudFront (Optional but recommended for Docs)
    if CF_DISTRIBUTION_ID and CF_DISTRIBUTION_ID != "your-dist-id":
        logging.info("Invalidating CloudFront for docs...")
        paths = [f"/{API_VERSION}/openapi.json", f"/{API_VERSION}/docs/*"]

        try:
            cf = boto3.client('cloudfront')
            cf.create_invalidation(
                DistributionId=CF_DISTRIBUTION_ID,
                InvalidationBatch={
                    'Paths': {'Quantity': len(paths), 'Items': paths},
                    'CallerReference': str(os.urandom(8))
                }
            )
            logging.info("✅ CloudFront invalidation submitted.")
        except Exception as e:
            logging.error(f"❌ CloudFront invalidation failed: {e}")

    logging.info("🎉 Documentation deployment complete!")
    logging.info(f"👉 API Spec: {API_BASE_URL}/openapi.json")
    logging.info(f"👉 UI Docs:  {API_BASE_URL}/api-docs.html")

if __name__ == "__main__":
    main()