angular.module('webApp')
    .controller('ContextsCtrl', function ($scope, $location, $resource) {
        $scope.sideMenu = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "active" : "";
        };

        $scope.tagClass = function(page) {
            var current = $location.hash() || 'overview';
            return page === current ? "show" : "hidden"
        };

        var api = $resource('/api/contexts/:contextName',
            {
                'contextName': '@ctxName',
                'num-cpu-cores': '@cores',
                'memory-per-node': '@memory'
            },
            {
                create: {method: 'POST', params: {}},
                stop: {method: 'DELETE', params: {}}
            }
        );

        $scope.tableRows = api.query();

        $scope.createContext = function() {
            if ($scope.ctxName) {
                var params = {};
                params['ctxName'] = $scope.ctxName;
                if ($scope.num_cpu_cores)
                    params['cores'] = $scope.num_cpu_cores;
                if ($scope.mem_per_node)
                    params['memory'] = $scope.mem_per_node;
                api.create(params, function(value, resp) {
                    $scope.uploadResult = 'Context Created Successfully!';
                }, function(resp) {
                    $scope.uploadResult = 'Create Context ' + $scope.ctxName + ' failed. Because ' + resp.data;
                });
            }
        };

        $scope.stopContext = function(cn) {
            api.stop({'contextName': cn}, function(value, resp) {
                $scope.uploadResult = 'Context Stopped Successfully!';
            }, function(resp) {
                $scope.uploadResult = 'Stop Context ' + cn + ' failed. Because ' + resp.data;
            });
            $scope.tableRows = api.query();
        }



    });