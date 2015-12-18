angular.module('webApp')
    .controller('ApplicationsCtrl', function ($scope, $location, $resource, Upload, $timeout) {
        $scope.sideMenu = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "active" : "";
        };

        $scope.tagClass = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "show" : "hidden"
        };

        var api = $resource('/api/jars');
        $scope.tableRows = api.query();

        $scope.uploadJar = function() {
            if ($scope.jarfile && $scope.appName) {
                $scope.upload($scope.jarfile)
            }
        };

        $scope.upload = function(file) {
            Upload.upload({
                url: '/api/jars/'.concat($scope.appName),
                data: { jar: file }
            }).then(function(resp) {
                $scope.uploadResult = 'Success ' + resp.config.data.name + ' uploaded.';
                console.log('Success ' + resp.config.data.name + 'uploaded. Response: ' + resp.data);
            }, function(resp) {
                $scope.uploadResult = 'Error status: ' + resp.status;
                console.log('Error status: ' + resp.status);
            }, function(evt) {
                var progressPercentage = parseInt(100.0 * evt.loaded / evt.total);
                console.log('progress: ' + progressPercentage + '% ' + evt.config.data.name);
            });
        };
    });