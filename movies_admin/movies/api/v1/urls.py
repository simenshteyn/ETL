from django.urls import path, include
from rest_framework.routers import SimpleRouter

from .views import MoviesViewSet

router = SimpleRouter()
router.register(r'movies', MoviesViewSet)

urlpatterns = [
    path('', include(router.urls))
]
