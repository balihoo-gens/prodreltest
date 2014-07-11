var app = angular.module('FulfillmentDashboard', ['ngRoute']);

toastr.options = {
	"closeButton" : true,
	"positionClass" : "toast-bottom-right",
	"timeOut" : "50000"
};

app.config(
	function($routeProvider) {
		$routeProvider
			.when("/workflow/:workflowId/run/:runId", {
			    controller : "workflowController",
			    templateUrl : "partials/workflow.html",
			    reloadOnSearch: false})
			.when("/history", {
				      controller : "historyController",
				      templateUrl : "partials/history.html",
				      reloadOnSearch: false})
			.otherwise({redirectTo : "/history"})
		;
	}
);

app.controller('historyController', function($scope, $route, $http, $location) {

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.getHistory();
		}
	);


	$scope.getHistory = function() {
		var params = {};
		$http.get('workflow/history', { params : params})
			.success(function(data) {
				         $scope.executions = data;
			         })
			.error(function(error) {
				       toastr.error(error.details, error.error)
			       });
	};

	$scope.seeWorkflow = function(execution) {
		$location.path("workflow/"+encodeURIComponent(execution.workflowId)+"/run/"+encodeURIComponent(execution.runId));
	};



});

app.controller('workflowController', function($scope, $route, $http, $location) {

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.params = $route.current.params;
			console.log($scope.params);
			$scope.getSections($scope.params.runId, $scope.params.workflowId);
		}
	);


	$scope.workflow = {};

	$scope.getSections = function(runId, workflowId) {
		var params = {};
		params['runId'] = runId;
		params['workflowId'] = workflowId;
		$http.get('workflow/sections', { params : params})
			.success(function(data) {
				         $scope.workflow = data;
			         })
			.error(function(error) {
				       toastr.error(error.details, error.error)
			       });
	};
});
