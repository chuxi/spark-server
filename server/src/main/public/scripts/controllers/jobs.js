angular.module('webApp')
    .controller('JobsCtrl', function ($scope, $location, $resource) {
        $scope.sideMenu = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "active" : "";
        };

        $scope.tagClass = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "show" : "hidden"
        };

        var api = $resource('/api/jobs/:jobId',
            {
                'jobId': '@id'
            });

        var setJobResultById = function(id) {
            var res = api.get({ 'jobId': id }, function() {
                $scope.job.result = res.result;
            }, function(error) {
                $scope.job.result = "Error, can not get result for " + error.data;
            });
        };

        $scope.tableRows = api.query(function() {
            $scope.job = $scope.tableRows[0];
            setJobResultById($scope.job.jobId);
        });


        $scope.showJobInfo = function(row) {
            $scope.job = row;
            setJobResultById($scope.job.jobId);
        };


        $scope.submitJob = function() {
            var params = {};
            if ($scope.appName && $scope.classPath) {
                params['appName'] = $scope.appName;
                params['classPath'] = $scope.classPath;
            }
            if ($scope.contextOpt) {
                params['contextOpt'] = $scope.contextOpt;
            }
            if ($scope.syncOpt) {
                params['syncOpt'] = $scope.syncOpt;
            }
            if ($scope.configString)  {
                params['config'] = $scope.configString;
            }


            api.save(params, function(value, resp) {
                $scope.uploadResult = 'Job Submitted Successfully!';
            }, function(resp) {
                $scope.uploadResult = 'Submit Job failed. Because ' + resp.data;
            });

        };
    });

