var app = angular.module('FulfillmentDashboard', ['ngRoute', 'ngSanitize']);

toastr.options = {
	"closeButton" : true,
	"positionClass" : "toast-bottom-right",
	"timeOut" : "50000"
};

app.factory('environment', function() {
	return {};
});

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
			.when("/workers", {
				      controller : "workersController",
				      templateUrl : "partials/workers.html",
				      reloadOnSearch: false})
			.otherwise({redirectTo : "/history"})
		;
	}
);

app.controller('envController', function($scope, $route, $http, $location, environment) {

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.init();
		}
	);


	$scope.init = function() {
		$http.get('workflow/environment', {})
			.success(function(data) {
				         $scope.environment = data;
			             environment.data = data;
			         })
			.error(function(error) {
				       toastr.error(error.details, error.error)
			       });
	};

});

app.controller('historyController', function($scope, $route, $http, $location) {

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.getHistory();
		}
	);


	$scope.getHistory = function() {
		$scope.loading = true;
		var params = {};
		$http.get('workflow/history', { params : params})
			.success(function(data) {
			             $scope.loading = false;
				         $scope.executions = data;
			         })
			.error(function(error) {
			           $scope.loading = false;
				       toastr.error(error.details, error.error)
			       });
	};

	$scope.seeWorkflow = function(execution) {
		$location.path("workflow/"+encodeURIComponent(execution.workflowId)+"/run/"+encodeURIComponent(execution.runId));
	};

	$scope.statusMap = {
		"FAILED" : "label-danger",
		"COMPLETED" : "label-success",
		"TIMED_OUT" : "label-warning"
	};

	$scope.figureStatusLabel = function(status) {
		return $scope.statusMap[status];
	};

	$scope.formatTag = function(tag) {
		var parts = tag.split(':');
		return "<b>"+parts[0]+"</b>&nbsp;:&nbsp;"+parts[1];
	};
});

app.controller('workflowController', function($scope, $route, $http, $location, $anchorScroll) {

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.params = $route.current.params;
			$scope.getWorkflow($scope.params.runId, $scope.params.workflowId);
		}
	);


	$scope.workflow = {};

	$scope.getWorkflow = function(runId, workflowId) {
		$scope.loading = true;
		var params = {};
		params['runId'] = runId;
		params['workflowId'] = workflowId;
		$http.get('workflow/detail', { params : params})
			.success(function(data) {
			             $scope.loading = false;
				         $scope.workflow = data;
			         })
			.error(function(error) {
			           $scope.loading = false;
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
		if($scope.isString(json)) {
			return $scope.div($scope.formatURLs(json), divclass);
		}

		return json;
	};

	$scope.formatJson = function(jsonString) {
		return $scope.jsonFormat(JSON.parse(jsonString), "json");
	};

	$scope.formatURLs = function(param) {
		var urlRegex = /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/ig;

		return param.replace(urlRegex, function (url) {

			if (( url.indexOf(".jpg") > 0 )
				|| ( url.indexOf(".png") > 0 )
				|| ( url.indexOf(".gif") > 0 )) {
				return '<img src="'+url+'">'+'<br/>'+url+'<br/>';
			}
			else {
				return '<a href="'+url+'">'+url+'</a>'+'<br/>';
			}
		});
	};

	$scope.isJSON = function(j) {
		return j[0] == '{';
	};

	$scope.isArray = function(a) {
		return a instanceof Array;
	};

	$scope.isString = function(s) {
		return typeof s === "string";
	};

	$scope.scrollToSection = function(name) {
		$location.hash("section_"+name);
		$anchorScroll();
	}

});

app.controller('workersController', function($scope, $route, $http, $location, environment) {

	$scope.showDefunct = false;

	$scope.$on(
		"$routeChangeSuccess",
		function($currentRoute, $previousRoute) {
			$scope.getWorkers();
		}
	);


	$scope.getWorkers = function() {
		$scope.loading = true;
		var params = {};
		$http.get('worker', { params : params})
			.success(function(data) {
				         $scope.loading = false;
				         $scope.workers = data;
			             $scope.categorizeWorkers();
			             $scope.currentDomain = environment.data.domain;
			         })
			.error(function(error) {
				       $scope.loading = false;
				       toastr.error(error.details, error.error)
			       });
	};

	$scope.setFreshnessLabel = function(worker) {

		worker.defunct = false;
		if(worker.minutesSinceLast < 2) {
			worker.freshness = "label-success";
		} else if(worker.minutesSinceLast < 5) {
			worker.freshness = "label-warning";
		} else {
			worker.freshness = "label-default";
			worker.defunct = true;
		}
	};

	$scope.categorizeWorkers = function() {
		$scope.domains = {};
		for(var w in $scope.workers) {
			var worker = $scope.workers[w];
			$scope.setFreshnessLabel(worker);

			if(!$scope.domains.hasOwnProperty(worker.domain)) {
				$scope.domains[worker.domain] = {};
			}
			var domain = $scope.domains[worker.domain];
			if(!domain.hasOwnProperty(worker.activityName)) {
				domain[worker.activityName] = [];
			}

			domain[worker.activityName].push(worker);

		}
	};

});
