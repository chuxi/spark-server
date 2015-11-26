angular.module('webApp')
    .controller('DashboardCtrl', function ($scope, $location, $resource) {
        $scope.sideMenu = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "active" : "";
        };

        $scope.tagClass = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "show" : "hidden"
        };



    });