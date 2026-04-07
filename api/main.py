from fastapi import FastAPI

from api.routes.health import router as health_router
from api.routes.feature_views import router as feature_views_router
from api.routes.latest import router as latest_router
from api.routes.training_sets import router as training_sets_router

app = FastAPI(title="mobility-feature-store")

app.include_router(health_router)
app.include_router(feature_views_router)
app.include_router(latest_router)
app.include_router(training_sets_router)
