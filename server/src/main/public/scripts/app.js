angular.module('webApp', [
    'ngRoute',
    'ngResource',
    'ngFileUpload'
]).config(function ($routeProvider) {
    $routeProvider.when('/', {
        templateUrl: '/assets/views/dashboard.html',
        controller: 'DashboardCtrl',
        controllerAs: 'dash'
    }).when('/applications', {
        templateUrl: '/assets/views/applications.html',
        controller: 'ApplicationsCtrl',
        controllerAs: 'apps'
    }).when('/contexts', {
        templateUrl: '/assets/views/contexts.html',
        controller: 'ContextsCtrl',
        controllerAs: 'ctxs'
    }).when('/jobs', {
        templateUrl: '/assets/views/jobs.html',
        controller: 'JobsCtrl',
        controllerAs: 'jobs'
    }).otherwise({
        redirectTo: '/'})
}).config(function($locationProvider) {
    $locationProvider.html5Mode({
        enabled: true,
        requireBase: false
    }).hashPrefix("!")
});

