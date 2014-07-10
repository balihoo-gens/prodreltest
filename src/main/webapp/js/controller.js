var app = angular.module('FulfillmentDashboard', ['ngRoute']);

toastr.options = {
	"closeButton" : true,
	"positionClass" : "toast-bottom-right"
};

app.config(
	function($routeProvider) {
		$routeProvider
//			.when("/workflow/:workflowId", {action : "workflow", reloadOnSearch: false})
			.otherwise({redirectTo : "/"})
		;
	}
);

app.controller('mainController', function($scope, $route, $http, $location) {

	$scope.message = "hey hey";

	$scope.query = function(test) {
		$http.get('worker', { params : { one : '1' }})
			.success(function(data) {
				$scope.message = data.message;
		                     })
			.error(function(error) {
			    $scope.message = error;
		                   });
	};
});
