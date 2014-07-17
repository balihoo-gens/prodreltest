var app = angular.module('FulfillmentDashboard', ['ngRoute', 'ngSanitize']);

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

	$scope.statusMap = {
		"FAILED" : "label-danger",
		"COMPLETED" : "label-success"
	};

	$scope.figureStatusLabel = function(status) {
		return $scope.statusMap[status];
	};

	$scope.formatTag = function(tag) {
		var parts = tag.split(':');
		return "<b>"+parts[0]+"</b>&nbsp;:&nbsp;"+parts[1];
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

	$scope.statusMap = {
		"INCOMPLETE" : "label-info",
		"SCHEDULED" : "label-info",
		"STARTED" : "label-info",
		"FAILED" : "label-warning",
		"TIMED OUT" : "label-warning",
		"CANCELED" : "label-warning",
		"TERMINAL" : "label-danger",
		"DISMISSED" : "label-primary",
		"COMPLETE" : "label-success",
		"CONTINGENT" : "label-default",
		"DEFERRED" : "label-info",
		"IMPOSSIBLE" : "label-danger"
	};

	$scope.figureStatusLabel = function(status) {
		return $scope.statusMap[status];
	};

	$scope.formatWhitespace = function(str) {
		return str.replace(/\n/g, '<br/>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;');
	};

	$scope.div = function(contents, classes) {
		return "<div class='"+classes+"'>"+contents+"</div>";
	};

	$scope.span = function(contents, classes) {
		return "<span class='"+classes+"'>"+contents+"</span>";
	};

	$scope.jsonFormat = function(json, divclass) {
		if(json instanceof Array) {
			var body = "";
			for(var item in json) {
				body += $scope.div($scope.jsonFormat(json[item]));
			}
			return $scope.div(body, "block "+divclass);
		}
		if(json instanceof Object) {
			var body = "";
			for(var key in json) {
				body += $scope.div("<span>"+key+"</span> : "+$scope.jsonFormat(json[key], "block"));
			}
			return $scope.div(body, "block "+divclass);
		}
		return $scope.div($scope.formatParameter(json), divclass);
	};

	$scope.formatParameter = function(param) {
		if(param[0] == '{') {
			return $scope.jsonFormat(JSON.parse(param), "json");
		}

		if(param instanceof Array) {
			var body = '';
			for(var item in param) {
				var sectionName = param[item];
				console.log(item);
				console.log(sectionName);
				if($scope.workflow.sections.hasOwnProperty(sectionName)) {
					body += $scope.span(sectionName, "label "+$scope.figureStatusLabel($scope.workflow.sections[sectionName].status)) + "&nbsp;"
				}
			}
			return body;
		}

		if(typeof param === "string") {
			var urlRegex = /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/ig;

			return param.replace(urlRegex, function (url) {

				if (( url.indexOf(".jpg") > 0 ) || ( url.indexOf(".png") > 0 ) || ( url.indexOf(".gif") > 0 )) {
					return '<img src="' + url + '">' + '<br/>' + url + '<br/>';
				}
				else {
					return '<a href="' + url + '">' + url + '</a>' + '<br/>'
				}
			});
		}

		return param;

	};
});
