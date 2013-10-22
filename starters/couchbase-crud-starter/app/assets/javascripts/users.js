angular.module("app", ["ngResource"])
  .factory('User', ["$resource", function($resource){
    return $resource('users/:id', { "id" : "@_id" }, { update: { method: 'PUT'}});
  }]).controller("UserCtrl", ["$scope", "User", 

  function($scope, User) {

    $scope.createForm = {};
    $scope.users = User.query();

    $scope.create = function() {
      var user = new User({name: $scope.createForm.name, age: $scope.createForm.age});
      user.$save(function(){
        $scope.createForm = {};
        $scope.users = User.query();
      })
    }

    $scope.remove = function(user) {
      user.$remove(function() {
        $scope.users = User.query();
      })
    }

    $scope.update = function(user) {
      user.$update(function() {
        $scope.users = User.query();
      })
    }
}]);
